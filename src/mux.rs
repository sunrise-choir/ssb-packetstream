// Note: this design of this was largely cribbed from the yamux crate:
// https://github.com/paritytech/yamux (MIT/Apache2.0)

use crate::*;

use core::pin::Pin;
use core::task::{Context, Poll, Poll::Pending, Poll::Ready};

use futures::channel::{mpsc, oneshot};
use futures::future;
use futures::io::{AsyncRead, AsyncWrite};
use futures::sink::{Sink, SinkExt};
use futures::stream::{FusedStream, Stream, StreamExt};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use tracing::{debug, instrument, trace};

#[derive(Debug, Snafu)]
pub enum Error {
    // TODO: useful context, better messages
    #[snafu(display("Connection closed"))]
    ConnectionClosed,

    #[snafu(display("PacketSink failure: {}", source))]
    Outgoing { source: sink::Error },

    #[snafu(display("PacketStream failure: {}", source))]
    Incoming { source: stream::Error },

    #[snafu(display("Failed to add outgoing packet to send queue: {}", source))]
    OutQueue { source: mpsc::SendError },

    #[snafu(display("Failed to send command from Handle to Connection: {}", source))]
    CommandSend { source: mpsc::SendError },

    #[snafu(display("Command response canceled: {}", source))]
    CommandResponse { source: oneshot::Canceled },

    #[snafu(display("Failed to send packet to ChildSink: {}", source))]
    ChildSinkSend { source: mpsc::SendError },

    #[snafu(display("Failed to send packet to ChildStream with id: {}", id))]
    ChildStreamSend { id: i32, source: mpsc::SendError },

    #[snafu(display("Failed to close ChildStream with id: {}", id))]
    ChildStreamClose { id: i32, source: mpsc::SendError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A mux(rpc) connection with a remote peer.
pub struct Connection<Id, R, W> {
    connection_id: Id,
    stream: PacketStream<R>,
    sink: PacketSink<W>,
    _next_id: i32,

    command_send: mpsc::Sender<Command>,
    command_recv: mpsc::Receiver<Command>,

    packet_send: mpsc::Sender<Packet>,
    packet_recv: mpsc::Receiver<Packet>,

    child_streams: HashMap<i32, mpsc::Sender<Packet>>,

    /// Only contains incoming request ids (positive numbers)
    closed_streams: HashSet<i32>,
}

impl<Id, R, W> Debug for Connection<Id, R, W>
where
    Id: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.connection_id)
            .field("child_streams", &self.child_streams.keys())
            .field("closed_streams", &self.closed_streams)
            .finish()
    }
}

impl<Id, R, W> Connection<Id, R, W>
where
    Id: Debug + Unpin + 'static,
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    /// Create a new mux connection, given an `AsyncRead` and an `AsyncWrite`.
    /// `id` can be any value of a type that implements `std::fmt::Debug`
    /// (eg the hostname of the remote peer); this id will be used in log
    /// messages.
    pub fn new(id: Id, r: R, w: W) -> Self {
        let (command_send, command_recv) = mpsc::channel(32);
        let (packet_send, packet_recv) = mpsc::channel(128);

        Connection {
            connection_id: id,
            stream: PacketStream::new(r),
            sink: PacketSink::new(w),
            _next_id: 1,
            command_send,
            command_recv,
            packet_send,
            packet_recv,
            child_streams: HashMap::new(),
            closed_streams: HashSet::new(),
        }
    }

    /// Create a new "handle" to the connection to provide some remote control.
    /// With a handle, you can create new outgoing requests, and close the connection.
    /// There can be many handles, and they can be cloned.
    pub fn handle(&self) -> Handle {
        Handle {
            command_send: self.command_send.clone(),
            packet_send: self.packet_send.clone(),
        }
    }

    /// `next_request()` is the "pump". Call it repeatedly
    /// to send, receive, and process packets.
    /// Returns a `ChildDuplex` stream for each new request that comes in from
    /// the remote peer. Returns `Err(Error::ConnectionClosed)` if the connection
    /// is closed by a `Handle` or by the remote peer.
    #[instrument]
    pub async fn next_request(&mut self) -> Result<ChildDuplex> {
        loop {
            let r = select_any(
                &mut self.command_recv,
                &mut self.packet_recv,
                &mut self.stream,
            )
            .await;

            if let Some((command, out_packet, in_packet)) = r {
                trace!(?command, ?out_packet, ?in_packet);
                if let Some(command) = command {
                    self.process_command(command).await?;
                }

                if let Some(p) = out_packet {
                    self.process_out_packet(p).await?;
                }

                match in_packet {
                    Some(Ok(p)) => {
                        if let Some(res) = self.process_in_packet(p).await? {
                            // New request, return child stream.
                            return Ok(res);
                        }
                        // Otherwise, we forwarded the packet to an existing child stream.
                    }
                    Some(Err(e)) => {
                        return Err(e).context(Incoming);
                    }
                    None => {}
                }
            } else {
                return Err(Error::ConnectionClosed);
            }
        }
    }
    fn next_id(&mut self) -> i32 {
        let id = self._next_id;

        // If the id overflows, reset to 1. TODO: check js behavior
        self._next_id = self._next_id.checked_add(1).unwrap_or(1);
        id
    }

    #[instrument]
    async fn process_command(&mut self, command: Command) -> Result<()> {
        match command {
            Command::NewRequest(is_stream, body_type, data, tx) => {
                let out_id = self.next_id();
                let in_id = -out_id;
                let (in_sink, in_stream) = mpsc::channel(32);

                let packet = Packet::new(is_stream, IsEnd::No, body_type, out_id, data);
                trace!(
                    "Creating new child stream for outgoing packet: {:?}",
                    packet
                );

                self.child_streams.insert(in_id, in_sink);
                self.sink.send(packet).await.context(Outgoing)?;
                tx.send(Ok(ChildStream {
                    id: in_id,
                    inner: in_stream,
                }));

                Ok(())
            }
            Command::CloseConnection(tx) => {
                debug!("Closing connection");
                // Close underlying packet sink (should send goodbye)
                self.sink.close().await.context(Outgoing)?;
                trace!("Closed PacketSink");

                // Close and drain packet channel
                self.packet_recv.close();
                while let Some(_) = self.packet_recv.next().await {}
                trace!("Closed packet channel");

                // Close child streams
                for (id, mut s) in self.child_streams.drain() {
                    trace!(id, "Closing child stream");
                    s.close().await.context(ChildStreamClose { id })?;
                }
                trace!("Closed child streams");

                // Close and drain command channel
                self.command_recv.close();
                while let Some(cmd) = self.command_recv.next().await {
                    match cmd {
                        Command::NewRequest(_, _, _, r) => {
                            let _ = r.send(Err(Error::ConnectionClosed));
                        }
                        Command::CloseConnection(r) => {
                            let _ = r.send(());
                        }
                    }
                }
                trace!("Drained command channel");
                let _ = tx.send(());
                Err(Error::ConnectionClosed)
            }
        }
    }

    #[instrument]
    async fn process_out_packet(&mut self, p: Packet) -> Result<()> {
        trace!(?p, "Sending packet");
        self.sink.send(p).await.context(Outgoing)
    }

    #[instrument]
    async fn process_in_packet(&mut self, p: Packet) -> Result<Option<ChildDuplex>> {
        if p.is_end() || (p.id < 0 && !p.is_stream()) {
            // Last packet for this stream id. We should have already created
            // a child stream for this id.

            let id = p.id;
            if let Some(mut child) = self.child_streams.remove(&id) {
                trace!("Rcvd final packet of stream id {}. Closing stream.", id);
                child.send(p).await.context(ChildStreamSend { id })?;
                child.close().await.context(ChildStreamClose { id })?;
                if id > 0 {
                    self.closed_streams.insert(id);
                }
            } else {
                trace!("Rcvd final packet for unknown or closed stream id: {}", id);
            }

            return Ok(None);
        }

        if let Some(ref mut child) = self.child_streams.get_mut(&p.id) {
            trace!("Found child stream for id: {}", p.id);
            let id = p.id;
            child.send(p).await.context(ChildStreamSend { id })?;
            return Ok(None);
        }

        if p.id < 0 {
            trace!("Rcvd unrecognized response packet id: {}", p.id);
            return Ok(None);
        }

        if self.closed_streams.contains(&p.id) {
            trace!("Rcvd packet for closed stream id: {}", p.id);
        }

        // New request.
        //
        // If !p.is_stream(), we could just return (Packet, ChildSink),
        // but I guess we'll just always make a duplex stream for now.

        trace!("New child stream");
        let id = p.id;
        let is_stream = p.stream;

        let (mut in_sink, in_stream) = mpsc::channel(32);
        in_sink.send(p).await.context(ChildStreamSend { id })?;

        if is_stream == IsStream::Yes {
            // Might be additional packets with the same id.
            self.child_streams.insert(id, in_sink);
        }

        Ok(Some(ChildDuplex {
            stream: ChildStream {
                id,
                inner: in_stream,
            },
            sink: ChildSink {
                id: -id,
                is_stream: is_stream,
                inner: self.packet_send.clone(),
            },
        }))
    }
}

enum Command {
    /// Create and send packet with next id, return ChildStream of reply packet(s).
    NewRequest(
        IsStream,
        BodyType,
        Vec<u8>,
        oneshot::Sender<Result<ChildStream>>,
    ),
    CloseConnection(oneshot::Sender<()>),
}

impl Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::NewRequest(i, b, v, _) => f
                .debug_tuple("NewRequest")
                .field(i)
                .field(b)
                .field(v)
                .finish(),

            Command::CloseConnection(_) => f.debug_tuple("CloseConnection").finish(),
        }
    }
}

/// Created from an existing connection, by calling `Connection::handle()`.
/// Used to create a new outgoing request, and to close the connection.
/// There can be many handles for a single connection, and each handle can be `clone()`d.
pub struct Handle {
    command_send: mpsc::Sender<Command>,
    packet_send: mpsc::Sender<Packet>,
}

impl Handle {
    /// Shuts down the mux::Connection
    #[instrument(skip(self))]
    pub async fn close_connection(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_send
            .send(Command::CloseConnection(tx))
            .await
            .context(CommandSend)?;
        trace!("Sent CloseConnection command");
        rx.await.context(CommandResponse)?;
        trace!("Got CloseConnection response");
        Ok(())
    }

    /// Send a packet to the remote peer, and create an incoming stream for the peer's
    /// response(s).
    ///
    /// Use this for "stream" (eg. createHistoryStream)
    /// and "async" (eg. blobs.has) muxrpc methods.
    pub async fn new_request(
        &mut self,
        is_stream: IsStream,
        body_type: BodyType,
        data: Vec<u8>,
    ) -> Result<ChildStream> {
        let (tx, rx) = oneshot::channel();
        self.command_send
            .send(Command::NewRequest(is_stream, body_type, data, tx))
            .await
            .context(CommandSend)?;
        rx.await.context(CommandResponse)?
    }

    /// Send a packet to the remote peer, create a sink for further outgoing packets
    /// and an incoming stream for the peer's response(s).
    pub async fn new_duplex(&mut self, body_type: BodyType, data: Vec<u8>) -> Result<ChildDuplex> {
        let stream = self.new_request(IsStream::Yes, body_type, data).await?;
        let id = stream.id;

        Ok(ChildDuplex {
            stream,
            sink: ChildSink {
                id: -id, // stream.id is id of incoming requests, which will be negative
                is_stream: IsStream::Yes,
                inner: self.packet_send.clone(),
            },
        })
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Handle {
            command_send: self.command_send.clone(),
            packet_send: self.packet_send.clone(),
        }
    }
}

/// Incoming (child) packet stream from remote peer.
#[derive(Debug)]
pub struct ChildStream {
    id: i32,
    inner: mpsc::Receiver<Packet>,
}

impl Stream for ChildStream {
    type Item = Packet;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl FusedStream for ChildStream {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

/// Outgoing (child) packet stream to remote peer.
#[derive(Debug)]
pub struct ChildSink {
    id: i32,
    is_stream: IsStream,
    inner: mpsc::Sender<Packet>,
}

impl Sink<(IsEnd, BodyType, Vec<u8>)> for ChildSink {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner)
            .poll_ready(cx)
            .map(|r| r.context(ChildSinkSend))
    }

    fn start_send(mut self: Pin<&mut Self>, item: (IsEnd, BodyType, Vec<u8>)) -> Result<()> {
        let (is_end, body_type, body) = item;
        let p = Packet::new(self.is_stream, is_end, body_type, self.id, body);
        Pin::new(&mut self.inner)
            .start_send(p)
            .context(ChildSinkSend)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map(|r| r.context(ChildSinkSend))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map(|r| r.context(ChildSinkSend))
    }
}

/// A `ChildStream` and `ChildSink` rolled into one.
/// Can be `.split()` into the two halves.
#[derive(Debug)]
pub struct ChildDuplex {
    stream: ChildStream,
    sink: ChildSink,
}

impl ChildDuplex {
    pub fn split(self) -> (ChildStream, ChildSink) {
        (self.stream, self.sink)
    }
}

impl Stream for ChildDuplex {
    type Item = Packet;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.stream.inner.poll_next_unpin(cx)
    }
}

impl Sink<(IsEnd, BodyType, Vec<u8>)> for ChildDuplex {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.sink).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: (IsEnd, BodyType, Vec<u8>)) -> Result<()> {
        Pin::new(&mut self.sink).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.sink).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.sink).poll_close(cx)
    }
}

/// Get the next element in a stream if one is immediately available.
/// Returns None if the stream has been closed or if poll_next() returns Pending.
fn take_next<S, I>(s: &mut S, cx: &mut Context) -> Option<I>
where
    S: Stream<Item = I> + FusedStream + Unpin,
{
    match s.poll_next_unpin(cx) {
        Pending => None,
        Ready(None) => None,
        Ready(Some(x)) => Some(x),
    }
}

async fn select_any<A, Ai, B, Bi, C, Ci>(
    a: &mut A,
    b: &mut B,
    c: &mut C,
) -> Option<(Option<Ai>, Option<Bi>, Option<Ci>)>
where
    A: Stream<Item = Ai> + FusedStream + Unpin,
    B: Stream<Item = Bi> + FusedStream + Unpin,
    C: Stream<Item = Ci> + FusedStream + Unpin,
    Ai: Debug,
    Bi: Debug,
    Ci: Debug,
{
    future::poll_fn(|cx: &mut Context| {
        if a.is_terminated() && b.is_terminated() && c.is_terminated() {
            return Ready(None);
        }

        let out = (take_next(a, cx), take_next(b, cx), take_next(c, cx));
        match out {
            (None, None, None) => Pending,
            _ => Ready(Some(out)),
        }
    })
    .await
}
