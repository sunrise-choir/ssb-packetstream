use core::pin::Pin;
use core::task::{Poll, Poll::Pending, Poll::Ready, Waker};
use byteorder::{ByteOrder, BigEndian};
use futures::io::{AsyncRead, AsyncReadExt};
use futures::stream::Stream;
use std::io::{Error, ErrorKind};

use crate::PinFut;
use crate::packet::*;

async fn recv<R: AsyncRead>(r: &mut R) -> Result<Option<Packet>, Error> {
    let mut head = [0; 9];
    let n = await!(r.read(&mut head))?;
    if n == 0 {
        return Err(Error::new(ErrorKind::UnexpectedEof,
                              "PacketStream underlying reader closed without goodbye"));
    }
    if n < head.len() {
        await!(r.read_exact(&mut head[n..]))?;
    }

    if &head == &[0u8; 9] {
        return Ok(None); // RPC goodbye
    }

    let body_len = BigEndian::read_u32(&head[1..5]);
    let id = BigEndian::read_i32(&head[5..]);

    let mut body = vec![0; body_len as usize];
    await!(r.read_exact(&mut body))?;

    Ok(Some(Packet::new(head[0].into(),
                        head[0].into(),
                        head[0].into(),
                        id,
                        body)))
}

async fn recv_move<R: AsyncRead + 'static>(mut r: R) -> (R, Result<Option<Packet>, Error>) {
    let res = await!(recv(&mut r));
    (r, res)
}

enum State<R> {
    Ready,
    Waiting(PinFut<(R, Result<Option<Packet>, Error>)>),
    Closed,
}

/// # Examples
/// ```rust
/// #![feature(async_await, await_macro, futures_api)]
///
/// use futures::executor::block_on;
/// use futures::prelude::{SinkExt, StreamExt};
/// use packetstream::*;
///
/// let p = Packet::new(IsStream::Yes,
///                     IsEnd::No,
///                     BodyType::Binary,
///                     12345,
///                     vec![1,2,3,4,5]);
///
/// let (writer, reader) = async_ringbuffer::ring_buffer(64);
///
/// let mut sink = PacketSink::new(writer);
/// let mut stream = PacketStream::new(reader);
/// block_on(async {
///     await!(sink.send(p));
///     let r = await!(stream.next()).unwrap().unwrap();
///     assert_eq!(&r.body, &[1,2,3,4,5]);
///     assert_eq!(r.id, 12345);
/// });
/// ```
pub struct PacketStream<R: AsyncRead> {
    reader: Option<R>,
    state: State<R>
}
impl<R: AsyncRead> PacketStream<R> {
    pub fn new(r: R) -> PacketStream<R> {
        PacketStream {
            reader: Some(r),
            state: State::Ready,
        }
    }

    pub fn is_closed(&self) -> bool {
        match &self.state {
            State::Closed => true,
            _ => false,
        }
    }

    pub fn into_inner(mut self) -> R {
        self.reader.take().unwrap()
    }
}

impl<R: AsyncRead + Unpin + 'static> Stream for PacketStream<R> {
    type Item = Result<Packet, Error>;

    fn poll_next(mut self: Pin<&mut Self>, wk: &Waker) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            State::Ready => {
                let r = self.reader.take().unwrap();
                self.state = State::Waiting(Box::pin(recv_move(r)));
                self.poll_next(wk)
            },
            State::Waiting(ref mut f) => {
                let p = Pin::as_mut(f);

                match p.poll(wk) {
                    Pending => Pending,
                    Ready((r, Ok(None))) => {
                        self.reader = Some(r);
                        self.state = State::Closed;
                        Ready(None)
                    },
                    Ready((r, Err(e))) => {
                        self.reader = Some(r);
                        self.state = State::Closed;
                        Ready(Some(Err(e)))
                    },
                    Ready((r, res)) => {
                        self.reader = Some(r);
                        self.state = State::Ready;
                        Ready(res.transpose())
                    }
                }
            },
            State::Closed => Ready(None),
        }
    }
}
