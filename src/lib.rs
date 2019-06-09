#![feature(async_await)]

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
    use byteorder::{BigEndian, ByteOrder};
    use futures::executor::block_on;
    use futures::io::AsyncReadExt;
    use futures::{future::join, stream::iter, SinkExt, TryStreamExt};

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
            let mut items = iter(msgs);
            sink.send_all(&mut items).await.unwrap();
            sink.close().await.unwrap();
        };

        let recv = async {
            let r: Vec<Packet> = stream
                .try_collect()
                .await.unwrap();
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

}
