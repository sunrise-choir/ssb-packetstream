#![feature(async_await, await_macro, futures_api)]

mod packet;
mod sink;
mod stream;

pub use packet::*;
pub use sink::*;
pub use stream::*;


#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{ByteOrder, BigEndian};
    use futures::executor::block_on;
    use futures::{stream::iter, join, SinkExt, StreamExt};

    #[test]
    fn encode() {
        let mut p = Packet::new(IsStream::Yes,
                                IsEnd::No,
                                BodyType::Json,
                                123,
                                vec![0; 25]);

        let expected_head: [u8; 9] = [0b0000_1010, 0, 0, 0, 25, 0, 0, 0, 123];
        assert_eq!(p.flags(), expected_head[0]);
        assert_eq!(p.header(), expected_head);

        p.is_end = IsEnd::Yes;
        p.is_stream = IsStream::No;
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

        let p = Packet::new(head[0].into(),
                            head[0].into(),
                            head[0].into(),
                            id,
                            vec![0; body_len as usize]);

        assert_eq!(p.header(), head);
    }


    #[test]
    fn sink_stream() {
        let msgs = vec![
            Packet::new(IsStream::Yes,
                        IsEnd::No,
                        BodyType::Binary,
                        10,
                        vec![1,2,3,4,5]),

            Packet::new(IsStream::No,
                        IsEnd::Yes,
                        BodyType::Utf8,
                        2002,
                        (0..50).collect()),

            Packet::new(IsStream::Yes,
                        IsEnd::Yes,
                        BodyType::Json,
                        12345,
                        (0..100).collect())
        ];

        let msgs_clone = msgs.clone();

        let (w, r) = async_ringbuffer::ring_buffer(1024);

        let mut sink = PacketSink::new(w);
        let stream = PacketStream::new(r);

        let send = async {
            let mut items = iter(msgs);
            await!(sink.send_all(&mut items)).unwrap();
            await!(sink.close()).unwrap();
        };

        let recv = async {
            let r: Vec<Packet> = await!(stream.map(|r| { dbg!(&r); r.unwrap() }).collect());
            r
        };

        let (_, received) = block_on(async { join!(send, recv) });

        for (i, msg) in received.iter().enumerate() {
            assert_eq!(msg, &msgs_clone[i]);
        }
    }
}
