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
        let mut msg = Packet {
            is_stream: IsStream::Yes,
            is_end: IsEnd::No,
            body_type: BodyType::Json,
            id: 123,
            body: vec![0; 25],
        };

        let expected_head: [u8; 9] = [0b0000_1010, 0, 0, 0, 25, 0, 0, 0, 123];
        assert_eq!(msg.flags(), expected_head[0]);
        assert_eq!(msg.header(), expected_head);

        msg.is_end = IsEnd::Yes;
        msg.is_stream = IsStream::No;
        msg.body_type = BodyType::Binary;
        assert_eq!(msg.flags(), 0b0000_0100);
    }

    #[test]
    fn decode() {

        let head: [u8; 9] = [0b0000_1101, 0, 0, 0, 25, 0, 0, 0, 200];
        let body_len = BigEndian::read_u32(&head[1..5]);
        let id = BigEndian::read_i32(&head[5..]);

        assert_eq!(body_len, 25);
        assert_eq!(id, 200);

        let msg = Packet {
            is_stream: head[0].into(),
            is_end: head[0].into(),
            body_type: head[0].into(),
            id,
            body: vec![0; body_len as usize],
        };

        assert_eq!(msg.header(), head);
    }


    #[test]
    fn sink_stream() {
        let msgs = vec![
            Packet {
                is_stream: IsStream::Yes,
                is_end: IsEnd::No,
                body_type: BodyType::Binary,
                id: 10,
                body: vec![1,2,3,4,5],
            },
            Packet {
                is_stream: IsStream::No,
                is_end: IsEnd::Yes,
                body_type: BodyType::Binary,
                id: 2002,
                body: (0..50).collect(),
            },
            Packet {
                is_stream: IsStream::Yes,
                is_end: IsEnd::No,
                body_type: BodyType::Binary,
                id: 12345,
                body: (0..100).collect(),
            }
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
