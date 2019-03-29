use core::future::Future;
use core::pin::Pin;
use core::task::{Poll, Poll::Pending, Poll::Ready, Waker};
use byteorder::{ByteOrder, BigEndian};
use futures::io::{AsyncRead, AsyncReadExt, Error};
use futures::stream::Stream;

use crate::packet::*;

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

/// # Examples
/// ```rust
/// #![feature(async_await, await_macro, futures_api)]
///
/// use futures::prelude::{SinkExt, StreamExt};
/// use packetstream::*;
///
/// let p = Packet {
///     is_stream: IsStream::Yes,
///     is_end: IsEnd::No,
///     body_type: BodyType::Binary,
///     id: 12345,
///     body: vec![1,2,3,4,5]
/// };
///
/// let (writer, reader) = async_ringbuffer::ring_buffer(64);
///
/// let mut sink = PacketSink::new(writer);
/// let mut stream = PacketStream::new(reader);
/// async {
///     await!(sink.send(p));
///     let r = await!(stream.next()).unwrap().unwrap();
///     assert_eq!(&r.body, &[1,2,3,4,5]);
///     assert_eq!(r.id, 12345);
/// };
/// ```
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
