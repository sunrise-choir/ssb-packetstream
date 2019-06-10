use crate::*;
use futures::channel::mpsc::{self, Receiver, Sender, SendError};
use futures::future::{join, FutureExt};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::sink::{SinkExt};
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

type DoneFuture<T> = Pin<Box<dyn Future<Output = Result<(), Error<T>>>>>;

pub struct MuxStream<R, HandlerErrorT>
where HandlerErrorT: error::Error + 'static,
{
    out_sender: Sender<Packet>,
    response_sinks: ChildSinkMap,
    continuation_sinks: ChildSinkMap,
    inner: Option<(PacketStream<R>, DoneFuture<HandlerErrorT>)>,
    error_t: PhantomData<HandlerErrorT>,
}

fn channel<T>() -> (Sender<T>, Receiver<T>) {
    mpsc::channel::<T>(128) // Arbitrary
}

/*

let mux = MuxStream::new(r, w);

async {
  mux.handle_incoming(async |p, inn, out| {

  })
}

async {
  let inn = mux.send(CreateHistStream(id, opts)).await?;
  inn.for_each(write_to_flume).await?;
}

async {
  let (inn, out) = mux.send_duplex(Ping).await?;
  let t0 = ms_since_epoch();
  out.send(t0).await?;
  let t1 = inn.next().await?;
}
*/

impl<R, E> MuxStream<R, E>
where
    R: AsyncRead + Unpin + 'static,
    E: error::Error + 'static,
{
    pub fn new<W>(r: R, w: W) -> MuxStream<R, E>
    where
        W: AsyncWrite + Unpin + 'static,
    {
        let (sender, receiver) = channel();
        let out_done = async {
            receiver.map(|p| Ok(p)).forward(PacketSink::new(w)).await.context(Outgoing)
        };

        MuxStream {
            out_sender: sender,
            response_sinks: child_sink_map(),
            continuation_sinks: child_sink_map(),
            inner: Some((PacketStream::new(r), out_done.boxed_local())),
            error_t: PhantomData,
        }
    }

    fn new_response_stream(&mut self, p: &Packet) -> ChildStream {
        let (in_sink, in_stream) = channel();
        self.response_sinks.lock().unwrap().insert(-p.id, in_sink);
        in_stream
    }

    pub async fn send(&mut self, packet: Packet) -> Result<ChildStream, Error<E>> {
        let stream = self.new_response_stream(&packet);
        self.out_sender.send(packet).await.context(Channel)?;
        Ok(stream)
    }

    pub async fn send_duplex<S>(&mut self, packet: Packet) -> Result<(ChildSink, ChildStream), Error<E>>
    where
        S: Stream<Item = Packet> + Unpin
    {
        let (inn_sink, inn) = channel();
        self.response_sinks.lock().unwrap().insert(packet.id, inn_sink);

        let mut out = self.out_sender.clone();
        out.send(packet).await.context(Channel)?;

        Ok((out, inn))
    }

    pub async fn run<H, T>(&mut self, handler: H) -> (Result<(), Error<E>>, Result<(), Error<E>>)
    where
        H: Fn(Packet, Sender<Packet>, Option<Receiver<Packet>>) -> T,
        T: Future<Output = Result<(), E>>,
    {
        let (in_stream, out_done) = self.inner.take().unwrap();
        let out = self.out_sender.clone();
        let response_sinks = self.response_sinks.clone();
        let continuation_sinks = self.continuation_sinks.clone();

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
                            eprintln!("forwarding packet to response sink");
                            sink.send(p).await.context(Channel)
                        }
                    } else {
                        eprintln!("Unhandled response packet: {:?}", p);
                        Ok(())
                    }
                } else {
                    dbg!(1);
                    let mut maybe_sink = {
                        let csinks = continuation_sinks.lock().unwrap();
                        csinks.get(&p.id).map(|s| s.clone())
                    };
                    dbg!(2);
                    if let Some(ref mut sink) = maybe_sink {
                        eprintln!("found continuation sink for id: {}", p.id);
                        if p.is_stream() && p.is_end() {
                            sink.close().await.context(Channel)
                        } else {
                            eprintln!("forwarding packet to response sink");
                            sink.send(p).await.context(Channel)
                        }
                    } else if p.is_stream() {
                        let (inn_sink, inn) = channel();
                        {
                            let mut csinks = continuation_sinks.lock().unwrap();
                            csinks.insert(p.id, inn_sink);
                        }
                        eprintln!("created new continuation sink for id: {}", p.id);
                        handler(p, out.clone(), Some(inn)).await.context(Handler)
                    } else {
                        handler(p, out.clone(), None).await.context(Handler)
                    }
                }
            });

        join(in_done, out_done).await
    }
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
    fn mux_one_side() {
        let (client_w, server_r) = async_ringbuffer::ring_buffer(2048);
        let (server_w, client_r) = async_ringbuffer::ring_buffer(2048);

        let mut pool = LocalPool::new();
        let mut spawner = pool.spawner();

        let mut server_mux = MuxStream::new(server_r, server_w);

        let done = spawner.spawn_local_with_handle(async move {
            server_mux.run(cool_rpc).await
        }).unwrap();

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

        let _p = pool.run_until(reply);

        // assert!(false);
        // let p = pool.run_until(done);
        // pool.run();
    }

}
