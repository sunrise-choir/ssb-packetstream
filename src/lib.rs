pub mod mux;
mod packet;
mod sink;
mod stream;

pub use packet::*;
pub use sink::*;
pub use stream::*;

use core::future::Future;
use core::pin::Pin;

type PinFut<O> = Pin<Box<dyn Future<Output = O> + 'static>>;

#[cfg(test)]
mod tests {
    use super::*;
    use async_ringbuffer as ringbuf;
    use byteorder::{BigEndian, ByteOrder};
    use futures::channel::mpsc;
    use futures::executor::block_on;
    use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite};
    use futures::stream::StreamExt;
    use futures::try_join;
    use futures::{future::join, stream::iter, SinkExt, TryStreamExt};
    use std::thread;
    use tracing::{debug, instrument, Level};
    use tracing_subscriber::FmtSubscriber;

    #[test]
    fn encode() {
        let mut p = Packet::new(IsStream::Yes, IsEnd::No, BodyType::Json, 123, vec![0; 25]);

        let expected_head: [u8; 9] = [0b0000_1010, 0, 0, 0, 25, 0, 0, 0, 123];
        assert_eq!(p.flags(), expected_head[0]);
        assert_eq!(p.header(), expected_head);

        p.end = IsEnd::Yes;
        p.stream = IsStream::No;
        p.body_type = BodyType::Binary;
        assert_eq!(p.flags(), 0b0000_0100);
    }

    #[test]
    fn decode() {
        let head: [u8; 9] = [0b0000_1101, 0, 0, 0, 25, 0, 0, 0, 200];
        let body_len = BigEndian::read_u32(&head[1..5]);
        let id = BigEndian::read_i32(&head[5..]);

        assert_eq!(body_len, 25);
        assert_eq!(id, 200);

        let p = Packet::new(
            head[0].into(),
            head[0].into(),
            head[0].into(),
            id,
            vec![0; body_len as usize],
        );

        assert_eq!(p.header(), head);
    }

    #[test]
    fn sink_stream() {
        let msgs = vec![
            Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Binary,
                10,
                vec![1, 2, 3, 4, 5],
            ),
            Packet::new(
                IsStream::No,
                IsEnd::Yes,
                BodyType::Utf8,
                2002,
                (0..50).collect(),
            ),
            Packet::new(
                IsStream::Yes,
                IsEnd::Yes,
                BodyType::Json,
                12345,
                (0..100).collect(),
            ),
        ];

        let msgs_clone = msgs.clone();

        let (w, r) = async_ringbuffer::ring_buffer(1024);

        let mut sink = PacketSink::new(w);
        let stream = PacketStream::new(r);

        let send = async {
            let mut items = iter(msgs).map(|m| Ok(m));
            sink.send_all(&mut items).await.unwrap();
            sink.close().await.unwrap();
        };

        let recv = async {
            let r: Vec<Packet> = stream.try_collect().await.unwrap();
            r
        };

        let (_, received) = block_on(async { join(send, recv).await });

        for (i, msg) in received.iter().enumerate() {
            assert_eq!(msg, &msgs_clone[i]);
        }
    }

    #[test]
    fn close() {
        let (w, r) = async_ringbuffer::ring_buffer(64);

        let mut sink = PacketSink::new(w);
        let mut stream = PacketStream::new(r);

        block_on(async {
            sink.send(Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Utf8,
                10,
                vec![1, 2, 3, 4, 5],
            ))
            .await
            .unwrap();

            let p = stream.try_next().await.unwrap().unwrap();
            assert!(p.is_stream());
            assert!(!p.is_end());
            assert_eq!(p.body_type, BodyType::Utf8);
            assert_eq!(p.id, 10);
            assert_eq!(&p.body, &[1, 2, 3, 4, 5]);

            sink.close().await.unwrap();

            let w = sink.into_inner();
            assert!(w.is_closed());

            let p = stream.try_next().await.unwrap();
            assert!(p.is_none());
            assert!(stream.is_closed());
        });
    }

    #[test]
    fn goodbye() {
        let (w, mut r) = async_ringbuffer::ring_buffer(64);

        let mut sink = PacketSink::new(w);

        block_on(async {
            sink.send(Packet::new(
                IsStream::Yes,
                IsEnd::No,
                BodyType::Utf8,
                10,
                vec![1, 2, 3, 4, 5],
            ))
            .await
            .unwrap();

            sink.close().await.unwrap();

            let mut tmp = [0; 14];
            let n = r.read(&mut tmp).await.unwrap();
            assert_eq!(n, 14);

            assert_eq!(&tmp, &[0b0000_1001, 0, 0, 0, 5, 0, 0, 0, 10, 1, 2, 3, 4, 5]);

            let mut head = [0; 9];
            let n = r.read(&mut head).await.unwrap();
            assert_eq!(n, 9);
            // goodbye header is 9 zeros
            assert_eq!(&head, &[0; 9]);

            let n = r.read(&mut head).await.unwrap();
            assert_eq!(n, 0);
        });
    }

    #[test]
    fn mux_with_threads() {
        let (client_sock, server_sock) = ringbuf::Duplex::pair(1024);
        let (client_r, client_w) = client_sock.split();
        let (server_r, server_w) = server_sock.split();

        let subscriber = FmtSubscriber::builder()
            // .with_max_level(Level::TRACE) // uncomment for lots of logs
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();

        block_on(async {
            assert!(try_join!(
                coolrpc_peer(1, client_r, client_w),
                coolrpc_peer(3, server_r, server_w)
            )
            .is_ok());
        });
    }

    const COOLRPC_CALL_COUNT: i32 = 3;

    #[instrument(skip(r, w))]
    async fn coolrpc_peer<R, W>(id: u8, r: R, w: W) -> Result<(), mux::Error>
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        let mut connection = mux::Connection::new(id, r, w);

        let mut threads = vec![];
        // To stop the test, we'll shut down the connection after
        // some number of requests.
        let (counter, mut counter_recv) = mpsc::channel::<i32>(16);
        let mut handle = connection.handle();
        threads.push(thread::spawn(move || {
            block_on(async {
                let mut count = 0;
                while let Some(i) = counter_recv.next().await {
                    count += i;
                    debug!(id, count);
                    if count == 2 * COOLRPC_CALL_COUNT {
                        handle.close_connection().await.unwrap();
                        return;
                    }
                }
            })
        }));

        // A `handle` can be used to close the connection,
        // and to send a new request to the remote peer.
        // There can be many handles, and handles can be cloned.
        let handle = connection.handle();
        let cntr = counter.clone();
        threads.push(thread::spawn(move || {
            block_on(async {
                send_coolrpc_requests(id, handle, cntr).await.unwrap();
            });
        }));

        // connection.next_request() is the "pump". Call it repeatedly
        // to send, receive, and process packets.
        while let Ok(req) = connection.next_request().await {
            let mut cntr = counter.clone();
            threads.push(thread::spawn(move || {
                block_on(async {
                    exec_coolrpc(id, req).await.unwrap();
                    cntr.send(1).await.unwrap()
                });
            }));
        }

        debug!("Exited next_request loop; joining child threads");
        threads.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        debug!("Joined child threads. coolrpc out.");
        Ok(())
    }

    #[instrument(skip(handle, counter))]
    async fn send_coolrpc_requests(
        id: u8,
        mut handle: mux::Handle,
        mut counter: mpsc::Sender<i32>,
    ) -> Result<(), mux::Error> {
        let mut call_count = 0;
        {
            let mut dbl1 = handle
                .new_request(
                    IsStream::No,
                    BodyType::Binary,
                    vec!['D' as u8, id, id * 10, id * 20, id * 30],
                )
                .await?;

            let result = dbl1.next().await.unwrap();
            assert_eq!(result.body, [id * 2, id * 20, id * 40, id * 60]);
            call_count += 1;
            debug!("Called Double (non-stream) procedure successfully.");
        }

        {
            let mut dbl2 = handle.new_duplex(BodyType::Binary, vec!['D' as u8]).await?;
            let mut dbl3 = handle.new_duplex(BodyType::Binary, vec!['D' as u8]).await?;
            call_count += 2;

            dbl2.send((IsEnd::No, BodyType::Binary, vec![id])).await?;
            dbl3.send((IsEnd::No, BodyType::Binary, vec![id * 2]))
                .await?;
            dbl2.send((IsEnd::No, BodyType::Binary, vec![id * 3]))
                .await?;
            dbl3.send((IsEnd::Yes, BodyType::Binary, vec![])).await?;
            dbl2.send((IsEnd::Yes, BodyType::Binary, vec![])).await?;

            assert_eq!(dbl2.next().await.unwrap().body, [id * 2]);
            assert_eq!(dbl2.next().await.unwrap().body, [id * 6]);
            assert_eq!(dbl3.next().await.unwrap().body, [id * 4]);
            // assert!(dbl3.is_terminated());
            // assert!(dbl2.is_terminated());

            debug!("Called Double (stream) procedure successfully.");
        }

        assert_eq!(call_count, COOLRPC_CALL_COUNT);
        counter.send(call_count).await.unwrap();

        Ok(())
    }

    #[instrument(skip(duplex))]
    async fn exec_coolrpc(id: u8, mut duplex: mux::ChildDuplex) -> Result<(), mux::Error> {
        let packet = duplex.next().await.unwrap();
        if packet.is_stream() && packet.is_end() {
            return Ok(());
        }

        let (method, args) = packet.body.split_first().unwrap();

        match *method as char {
            'R' => {
                // Reverse
                debug!(?args, "Reverse");
                let reversed = args.iter().rev().map(|u| *u).collect();
                duplex.send((IsEnd::No, BodyType::Binary, reversed)).await?
            }
            'D' => {
                // Double each arg
                if !packet.is_stream() {
                    debug!(?args, "Double (non-stream)");
                    let doubled = args.iter().map(|n| n * 2).collect();
                    duplex.send((IsEnd::No, BodyType::Binary, doubled)).await?
                } else {
                    debug!(?args, "Double (stream)");
                    let (mut stream, mut sink) = duplex.split();

                    while let Some(p) = stream.next().await {
                        if p.is_end() {
                            return Ok(());
                        }
                        let x = p.body.first().unwrap();
                        sink.send((IsEnd::No, BodyType::Binary, vec![x * 2]))
                            .await?;
                    }
                }
            }
            _ => {
                debug!(?packet, "CoolRPC unrecognized packet");
            }
        }
        debug!("done");
        Ok(())
    }
}
