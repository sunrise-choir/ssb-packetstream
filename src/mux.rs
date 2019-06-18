use crate::*;
use futures::channel::mpsc::{self, Receiver, Sender, SendError};
use futures::future::{join, FutureExt};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::sink::{Sink, SinkExt};
use futures::io::{AsyncRead, AsyncWrite};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::marker::PhantomData;
use std::error;

use snafu::{futures::TryStreamExt as SnafuTSE, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error<HandlerErrorT>
where HandlerErrorT: std::error::Error + 'static
{

    #[snafu(display("PacketSink failure: {}", source))]
    Outgoing {
        source: sink::Error
    },

    #[snafu(display("PacketStream failure: {}", source))]
    Incoming {
        source: stream::Error
    },

    #[snafu(display("Error sending packet: {}", source))]
    Channel {
        source: SendError
    },

    #[snafu(display("Error sending packet: {}", source))]
    Handler {
        source: HandlerErrorT
    },
}

// Packets with id < 0 are responses to one of our
// outgoing requests.
// Packets with id > 0 may be incoming requests, or may be
// continuations of an incoming stream.
type ChildSink = Sender<Packet>;
type ChildStream = Receiver<Packet>;
type ChildSinkMap = Arc<Mutex<HashMap<i32, ChildSink>>>;

fn child_sink_map() -> ChildSinkMap {
    Arc::new(Mutex::new(HashMap::new()))
}

pub struct MuxSender<E> {
    inner: Sender<Packet>,
    _id: i32,
    response_sinks: ChildSinkMap,
    phantom: PhantomData<E>,
}

impl<E> MuxSender<E> {
    fn new(inner: Sender<Packet>, response_sinks: ChildSinkMap) -> MuxSender<E> {
        MuxSender {
            inner,
            _id: 1,
            response_sinks,
            phantom: PhantomData,
        }
    }

    fn next_id(&mut self) -> i32 {
        let id = self._id;

        // TODO: check js behavior
        self._id = match self._id.checked_add(1) {
            None => 1,
            Some(i) => i
        };
        id
    }

    fn new_response_stream(&mut self, request_id: i32) -> ChildStream {
        let (in_sink, in_stream) = channel();
        self.response_sinks.lock().unwrap().insert(-request_id, in_sink);
        in_stream
    }

    pub fn close(&mut self) {
        self.inner.close_channel();
    }
}

impl<E> MuxSender<E>
where
    E: error::Error + 'static,
{
    pub async fn send(&mut self, body_type: BodyType, body: Vec<u8>) -> Result<ChildStream, Error<E>> {
        let out_id = self.next_id();
        let in_stream = self.new_response_stream(out_id);

        let p = Packet::new(IsStream::No, IsEnd::No, body_type, out_id, body);
        self.inner.send(p).await.context(Channel)?;
        Ok(in_stream)
    }

    pub async fn send_duplex(&mut self, body_type: BodyType, body: Vec<u8>) -> Result<(MuxChildSender<E>, ChildStream), Error<E>> {
        let out_id = self.next_id();
        let in_stream = self.new_response_stream(out_id);

        let mut out = MuxChildSender {
            id: out_id,
            is_stream: IsStream::Yes,
            sink: self.inner.clone(),
            phantom: PhantomData,
        };
        out.send(body_type, body).await?;

        Ok((out, in_stream))
    }
}

impl<E> Drop for MuxSender<E> {
    fn drop(&mut self) {
        self.close();
    }
}

// TODO: name
pub struct MuxChildSender<E> {
    id: i32,
    is_stream: IsStream,
    sink: Sender<Packet>,
    phantom: PhantomData<E>,
}
impl<E> MuxChildSender<E>
where E: error::Error + 'static,
{
    fn new(id: i32, is_stream: IsStream, sink: Sender<Packet>) -> MuxChildSender<E> {
        MuxChildSender {
            id,
            is_stream,
            sink,
            phantom: PhantomData,
        }
    }

    pub async fn send(&mut self, body_type: BodyType, body: Vec<u8>) -> Result<(), Error<E>> {
        self.send_packet(Packet::new(self.is_stream,
                                     IsEnd::No,
                                     body_type,
                                     self.id,
                                     body)).await
    }

    pub async fn send_end(&mut self, body_type: BodyType, body: Vec<u8>) -> Result<(), Error<E>> {
        self.send_packet(Packet::new(self.is_stream,
                                     IsEnd::Yes,
                                     body_type,
                                     self.id,
                                     body)).await
    }

    pub async fn send_all<S>(&mut self, s: S) -> Result<(), Error<E>>
    where S: Stream<Item = (BodyType, Vec<u8>)> + Unpin,
    {
        let id = self.id;
        let is_stream = self.is_stream;

        // TODO: better error context
        let mut stream = s.map(|(bt, b)| Packet::new(is_stream, IsEnd::No, bt, id, b));
        self.sink.send_all(&mut stream).await.context(Channel)
    }

    async fn send_packet(&mut self, p: Packet) -> Result<(), Error<E>> {
        self.sink.send(p).await.context(Channel)
    }
}

// Inbound side needs:
//   - &mut response_sink_map     (for ids < 0)
//   - &mut continuation_sink_map (for ids > 0)
//   - out_sender (owned clone)
// returns:
//   - input completion future?
// ! need to provide a way to shutdown
//
// Outbound side needs:
//   - &mut response_sink_map
//   - out_sender (owned clone)

pub fn mux<R, W, H, E, T>(r: R, w: W, handler: H)
                          -> (MuxSender<E>, impl Future<Output = Result<(), Error<E>>>)
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
    H: Fn(Packet, Sender<Packet>, Option<Receiver<Packet>>) -> T,
    T: Future<Output = Result<(), E>>,
    E: error::Error + 'static,
{

    let (shared_sender, recv) = channel();
    let out_done = async move {
        let r = recv.map(|p| Ok(p)).forward(PacketSink::new(w)).await.context(Outgoing);
        eprintln!("FORWARDED ALL");
        r
    };

    let response_sinks = child_sink_map();
    let continuation_sinks = child_sink_map();

    let sender = MuxSender::new(shared_sender.clone(), response_sinks.clone());

    let done = async move {
        let in_stream = PacketStream::new(r);

        const OPEN_STREAMS_LIMIT: usize = 128;
        let in_done = in_stream.context(Incoming)
            .try_for_each_concurrent(OPEN_STREAMS_LIMIT, |p| async {
                eprintln!("mux received: {:?}", p);
                if p.id < 0 {
                    let mut response_sinks = response_sinks.lock().unwrap();
                    if let Some(ref mut sink) = response_sinks.get_mut(&p.id) {
                        if p.is_stream() && p.is_end() {
                            sink.close().await.context(Channel)
                        } else {
                            sink.send(p).await.context(Channel)
                        }
                    } else {
                        eprintln!("Unhandled response packet: {:?}", p);
                        Ok(())
                    }
                } else {
                    let mut maybe_sink = {
                        let csinks = continuation_sinks.lock().unwrap();
                        csinks.get(&p.id).map(|s| s.clone())
                    };
                    if let Some(ref mut sink) = maybe_sink {
                        eprintln!("found continuation sink for id: {}", p.id);
                        if p.is_stream() && p.is_end() {
                            sink.close().await.context(Channel)
                        } else {
                            sink.send(p).await.context(Channel)
                        }
                    } else if p.is_stream() {
                        let (inn_sink, inn) = channel();
                        {
                            let mut csinks = continuation_sinks.lock().unwrap();
                            csinks.insert(p.id, inn_sink);
                        }
                        eprintln!("created new continuation sink for id: {}", p.id);
                        handler(p, shared_sender.clone(), Some(inn)).await.context(Handler)
                    } else {
                        handler(p, shared_sender.clone(), None).await.context(Handler)
                    }
                }
            }).map(|r| { eprintln!("IN_DONE"); r });
        let (in_r, out_r) = join(in_done, out_done).await;
        in_r.or(out_r)
    };
    (sender, done)
}

fn channel<T>() -> (Sender<T>, Receiver<T>) {
    mpsc::channel::<T>(128) // Arbitrary
}


#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::LocalPool;
    use futures::task::LocalSpawnExt;
    use futures::stream::StreamExt;
    use snafu::{ResultExt, Snafu};


    #[derive(Debug, Snafu)]
    enum RpcError {
        #[snafu(display("Failed to send Reverse response: {}", source))]
        Reverse {
            source: SendError
        },

        #[snafu(display("Failed to send Double (non-stream) response: {}", source))]
        DoubleBatch {
            source: SendError
        },

        #[snafu(display("Failed to send Double (stream) response: {}", source))]
        DoubleStream {
            source: SendError
        },
    }

    async fn cool_rpc(p: Packet, mut out: Sender<Packet>, inn: Option<Receiver<Packet>>)
                      -> Result<(), RpcError> {

        let (method, args) = p.body.split_first().unwrap();

        match *method as char {
            'R' => { // Reverse
                out.send(Packet::new(
                    IsStream::No,
                    IsEnd::Yes,
                    BodyType::Binary,
                    -p.id,
                    args.iter().rev().map(|u| *u).collect(),
                )).await.context(Reverse)
            },
            'D' => { // Double each arg
                if p.is_stream() {
                    let inn = inn.unwrap();
                    inn.map(|argp: Packet| {
                        let x = argp.body.first().unwrap();
                        Ok(Packet::new(
                            IsStream::Yes,
                            IsEnd::No,
                            BodyType::Binary,
                            -argp.id,
                            vec![x * 2],
                        ))
                    }).forward(out).await.context(DoubleStream)
                } else {
                    out.send(Packet::new(
                        IsStream::No,
                        IsEnd::Yes,
                        BodyType::Binary,
                        -p.id,
                        args.iter().map(|n| n * 2).collect(),
                    )).await.context(DoubleBatch)
                }
            },
            _ => {
                eprintln!("CoolRPC unrecognized packet: {:?}", p);
                Ok(())
            }
        }
    }

    #[test]
    fn mux_one_way() {
        let (client_w, server_r) = async_ringbuffer::ring_buffer(2048);
        let (server_w, client_r) = async_ringbuffer::ring_buffer(2048);

        let mut pool = LocalPool::new();
        let mut spawner = pool.spawner();

        let (mut server_out, server_done) = mux(server_r, server_w, cool_rpc);

        let server_done = spawner.spawn_local_with_handle(server_done).unwrap();

        let mut client_in = PacketStream::new(client_r);
        let mut client_out = PacketSink::new(client_w);

        let reply = spawner.spawn_local_with_handle(async move {
            // Call Reverse procedure
            client_out.send(Packet::new(
                IsStream::No,
                IsEnd::No,
                BodyType::Binary,
                1,
                vec!['R' as u8, 1, 2, 3, 4, 5],
            )).await.unwrap();

            let p = client_in.try_next().await.unwrap().unwrap();
            assert_eq!(p.id, -1);
            assert_eq!(p.body, &[5, 4, 3, 2, 1]);

            // Call Double (non-stream) procedure
            client_out.send(Packet::new(
                IsStream::No,
                IsEnd::No,
                BodyType::Binary,
                2,
                vec!['D' as u8, 0, 5, 10, 20, 30],
            )).await.unwrap();

            let p = client_in.try_next().await.unwrap().unwrap();
            assert_eq!(p.id, -2);
            assert_eq!(p.body, &[0, 10, 20, 40, 60]);

            // Call Double (stream) procedure
            client_out.send(Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Binary,
                3,
                vec!['D' as u8],
            )).await.unwrap();
            client_out.send(Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Binary,
                3,
                vec![6],
            )).await.unwrap();

            let p = client_in.try_next().await.unwrap().unwrap();
            assert_eq!(p.id, -3);
            assert_eq!(p.body, &[12]);

            client_out.send(Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Binary,
                3,
                vec![8],
            )).await.unwrap();
            client_out.send(Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Binary,
                3,
                vec![10],
            )).await.unwrap();

            let p = client_in.try_next().await.unwrap().unwrap();
            assert_eq!(p.id, -3);
            assert_eq!(p.body, &[16]);

            let p = client_in.try_next().await.unwrap().unwrap();
            assert_eq!(p.id, -3);
            assert_eq!(p.body, &[20]);

        }).unwrap();

        pool.run_until(reply);

        server_out.close();
        pool.run_until(server_done).unwrap();
    }

    fn sb_packet(b: u8, id: i32) -> Packet {
        Packet::new(
            IsStream::Yes,
            IsEnd::No,
            BodyType::Binary,
            id,
            vec![b],
        )
    }

    #[test]
    fn mux_two_way() {
        let (client_w, server_r) = async_ringbuffer::ring_buffer(2048);
        let (server_w, client_r) = async_ringbuffer::ring_buffer(2048);

        let mut pool = LocalPool::new();
        let mut spawner = pool.spawner();

        let (mut server_out, server_done) = mux(server_r, server_w, cool_rpc);
        let (mut client_out, client_done) = mux(client_r, client_w, cool_rpc);

        let _server_done = spawner.spawn_local_with_handle(server_done).unwrap();
        let _client_done = spawner.spawn_local_with_handle(client_done).unwrap();

        let reply = spawner.spawn_local_with_handle(async move {
            // Call Reverse procedure
            let rev_id = 1;
            let mut rev_response = client_out.send(BodyType::Binary,
                                                   vec!['R' as u8, 1, 2, 3, 4, 5])
                .await.unwrap();

            // Call Double (stream) procedure
            let a_id = 2;
            let (mut a_out, mut a_in) = client_out
                .send_duplex(BodyType::Binary, vec!['D' as u8])
                .await.unwrap();

            a_out.send(BodyType::Binary, vec![6]).await.unwrap();

            let p = a_in.next().await.unwrap();
            assert_eq!(p.id, -a_id);
            assert_eq!(p.body, &[12]);

            // Check rev response
            let p = rev_response.next().await.unwrap();
            assert_eq!(p.id, -rev_id);
            assert_eq!(p.body, &[5, 4, 3, 2, 1]);

            a_out.send(BodyType::Binary, vec![8]).await.unwrap();

            let (mut dub3_out, mut dub3_in) = server_out
                .send_duplex(BodyType::Binary, vec!['D' as u8])
                .await.unwrap();
            dub3_out.send(BodyType::Binary, vec![30]).await.unwrap();

            let b_id = 3;
            let (mut b_out, mut b_in) = client_out
                .send_duplex(BodyType::Binary, vec!['D' as u8])
                .await.unwrap();
            b_out.send(BodyType::Binary, vec![44]).await.unwrap();
            a_out.send(BodyType::Binary, vec![10]).await.unwrap();
            dub3_out.send(BodyType::Binary, vec![30]).await.unwrap();

            let p = dub3_in.next().await.unwrap();
            assert_eq!(p.id, -1);
            assert_eq!(p.body, &[60]);

            let p = b_in.next().await.unwrap();
            assert_eq!(p.id, -b_id);
            assert_eq!(p.body, &[88]);

            let p = a_in.next().await.unwrap();
            assert_eq!(p.id, -a_id);
            assert_eq!(p.body, &[16]);

            b_out.send(BodyType::Binary, vec![100]).await.unwrap();

            let p = b_in.next().await.unwrap();
            assert_eq!(p.id, -b_id);
            assert_eq!(p.body, &[200]);

            let p = a_in.next().await.unwrap();
            assert_eq!(p.id, -a_id);
            assert_eq!(p.body, &[20]);

            server_out.close();
        }).unwrap();

        pool.run_until(reply);

        // // XXX: server_done doesn't finish
        // let p = pool.run_until(server_done);
        // let p = pool.run_until(client_done);
    }

}
