#![feature(async_await, await_macro, futures_api)]

use core::future::Future;
use core::pin::Pin;
use core::task::{Poll, Poll::Pending, Poll::Ready, Waker};
use byteorder::{ByteOrder, BigEndian};
use futures::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt, Error};
use futures::stream::Stream;
use futures::sink::Sink;

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum IsStream {
    No  = 0,
    Yes = 0b0000_1000,
}
impl From<u8> for IsStream {
    fn from(u: u8) -> IsStream {
        match u & IsStream::Yes as u8 {
            0 => IsStream::No,
            _ => IsStream::Yes
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum IsEnd {
    No  = 0,
    Yes = 0b0000_0100,
}
impl From<u8> for IsEnd {
    fn from(u: u8) -> IsEnd {
        match u & IsEnd::Yes as u8 {
            0 => IsEnd::No,
            _ => IsEnd::Yes
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BodyType {
    Binary = 0b00,
    Utf8 = 0b01,
    Json = 0b10,
}
impl From<u8> for BodyType {
    fn from(u: u8) -> BodyType {
        match u & 0b11 {
            0b00 => BodyType::Binary,
            0b01 => BodyType::Utf8,
            0b10 => BodyType::Json,
            _ => panic!(),
        }
    }
}

async fn send<W: AsyncWrite + 'static>(mut w: W, msg: Packet) -> (W, Result<(), Error>) {
    let h = msg.header();
    let mut r = await!(w.write_all(&h));
    if r.is_ok() {
        r = await!(w.write_all(&msg.body));
    }
    (w, r.map(|_| ()))
}

#[derive(Clone, Debug, PartialEq)]
pub struct Packet {
    is_stream: IsStream,
    is_end: IsEnd,
    body_type: BodyType,
    id: i32,
    body: Vec<u8>,
}
impl Packet {
    fn flags(&self) -> u8 {
        self.is_stream as u8 | self.is_end as u8 | self.body_type as u8
    }
    fn header(&self) -> [u8; 9] {
        let mut header = [0; 9];
        header[0] = self.flags();
        BigEndian::write_u32(&mut header[1..5], self.body.len() as u32);
        BigEndian::write_i32(&mut header[5..], self.id);
        header
    }
}

async fn recv<R: AsyncRead>(r: &mut R) -> Result<Option<Packet>, Error> {
    let mut head = [0; 9];
    let n = await!(r.read(&mut head))?;
    if n == 0 {
        return Ok(None);
    }
    if n < head.len() {
        await!(r.read_exact(&mut head[n..]))?;
    }

    let body_len = BigEndian::read_u32(&head[1..5]);
    let id = BigEndian::read_i32(&head[5..]);

    let mut body = vec![0; body_len as usize];
    await!(r.read_exact(&mut body))?;

    Ok(Some(Packet {
        is_stream: head[0].into(),
        is_end: head[0].into(),
        body_type: head[0].into(),
        id,
        body,
    }))
}

async fn recv_move<R: AsyncRead + 'static>(mut r: R) -> (R, Result<Option<Packet>, Error>) {
    let res = await!(recv(&mut r));
    (r, res)
}

type PinFut<O> = Pin<Box<dyn Future<Output=O> + 'static>>;

pub struct PacketStream<R> {
    reader: Option<R>,
    future: Option<PinFut<(R, Result<Option<Packet>, Error>)>>
}
impl<R> PacketStream<R> {
    pub fn new(r: R) -> PacketStream<R> {
        PacketStream {
            reader: Some(r),
            future: None,
        }
    }
}

impl<R: AsyncRead + Unpin + 'static> Stream for PacketStream<R> {
    type Item = Result<Packet, Error>;

    fn poll_next(mut self: Pin<&mut Self>, wk: &Waker) -> Poll<Option<Self::Item>> {
        match &mut self.future {
            None => {
                let r = self.reader.take().unwrap();
                self.future = Some(Box::pin(recv_move(r)));
                self.poll_next(wk)
            },
            Some(ref mut f) => {
                let p = Pin::as_mut(f);

                match p.poll(wk) {
                    Pending => Pending,
                    Ready((r, res)) => {
                        self.reader = Some(r);
                        self.future = None;
                        Ready(res.transpose())
                    }
                }
            }
        }
    }
}

enum SinkState<W> {
    Ready,
    Sending(PinFut<(W, Result<(), Error>)>),
    Closing,
}

pub struct PacketSink<W> {
    writer: Option<W>,
    state: SinkState<W>
}
impl<W> PacketSink<W> {
    pub fn new(w: W) -> PacketSink<W> {
        PacketSink {
            writer: Some(w),
            state: SinkState::Ready,
        }
    }

    fn do_poll_flush(&mut self, wk: &Waker) -> Poll<Result<(), Error>> {
        match &mut self.state {
            SinkState::Ready => Ready(Ok(())),
            SinkState::Sending(ref mut f) => {
                let p = Pin::as_mut(f);

                match p.poll(wk) {
                    Pending => Pending,
                    Ready((w, res)) => {
                        self.writer = Some(w);
                        self.state = SinkState::Ready;
                        Ready(res)
                    }
                }
            },
            SinkState::Closing => panic!() // TODO?
        }
    }
}

impl<W> Sink for PacketSink<W>
    where W: AsyncWrite + Unpin + 'static
{
    type SinkItem = Packet;
    type SinkError = Error;

    fn poll_ready(self: Pin<&mut Self>, wk: &Waker) -> Poll<Result<(), Self::SinkError>> {
        self.poll_flush(wk)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Self::SinkItem) -> Result<(), Self::SinkError> {
        let w = self.writer.take().unwrap();
        self.state = SinkState::Sending(Box::pin(send(w, item)));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, wk: &Waker) -> Poll<Result<(), Self::SinkError>> {
        self.do_poll_flush(wk)
    }

    fn poll_close(mut self: Pin<&mut Self>, wk: &Waker) -> Poll<Result<(), Self::SinkError>> {
        match self.state {
            SinkState::Ready => {
                self.state = SinkState::Closing;
                self.poll_close(wk)
            },
            SinkState::Sending(_) => match self.do_poll_flush(wk) {
                Pending => Pending,
                Ready(_) => {
                    self.state = SinkState::Closing;
                    self.poll_close(wk)
                }
            },
            SinkState::Closing => {
                if let Some(ref mut w) = &mut self.writer {
                    w.poll_close(wk)
                } else {
                    panic!()
                }
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
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
