use core::pin::Pin;
use core::task::{Context, Poll, Poll::Pending, Poll::Ready};
use futures::io::{AsyncWrite, AsyncWriteExt, Error};
use futures::sink::Sink;

use crate::PinFut;
use crate::packet::*;

async fn send<W>(mut w: W, msg: Packet) -> (W, Result<(), Error>)
where W: AsyncWrite + Unpin + 'static
{
    let h = msg.header();
    let mut r = await!(w.write_all(&h));
    if r.is_ok() {
        r = await!(w.write_all(&msg.body));
    }
    (w, r.map(|_| ()))
}

async fn send_goodbye<W>(mut w: W) -> (W, Result<(), Error>)
where W: AsyncWrite + Unpin + 'static
{
    let r = await!(w.write_all(&[0; 9]));
    (w, r.map(|_| ()))
}

enum State<W> {
    Ready,
    Sending(PinFut<(W, Result<(), Error>)>),
    SendingGoodbye(PinFut<(W, Result<(), Error>)>),
    Closing,
}

/// #Examples
/// ```rust
/// #![feature(async_await, await_macro, futures_api)]
///
/// use std::io::Cursor;
/// use futures::executor::block_on;
/// use futures::prelude::SinkExt;
/// use packetstream::*;
///
/// let mut sink = PacketSink::new(Cursor::new(vec![0; 14]));
/// block_on(async {
///     await!(sink.send(Packet::new(IsStream::Yes,
///                                  IsEnd::No,
///                                  BodyType::Json,
///                                  123,
///                                  vec![1,2,3,4,5])));
///     await!(sink.close());
///     let buf = sink.into_inner().into_inner();
///     assert_eq!(&buf, &[0b0000_1010, 0, 0, 0, 5, 0, 0, 0, 123, 1, 2, 3, 4, 5]);
/// });
/// ```
pub struct PacketSink<W: AsyncWrite> {
    writer: Option<W>,
    state: State<W>
}
impl<W> PacketSink<W>
where W: AsyncWrite + Unpin
{
    pub fn new(w: W) -> PacketSink<W> {
        PacketSink {
            writer: Some(w),
            state: State::Ready,
        }
    }

    fn do_poll_flush(&mut self, cx: &mut Context) -> Poll<Result<(), Error>> {
        match &mut self.state {
            State::Ready => {
                if let Some(ref mut w) = &mut self.writer {
                    Pin::new(w).poll_flush(cx)
                } else {
                    panic!()
                }
            },
            State::Sending(ref mut f) => {
                let p = Pin::as_mut(f);

                match p.poll(cx) {
                    Pending => Pending,
                    Ready((w, _res)) => {
                        // TODO: check if 'res' is an error
                        self.writer = Some(w);
                        self.state = State::Ready;
                        self.do_poll_flush(cx)
                    }
                }
            },
            _ => panic!() // I guess?
        }
    }

    pub fn into_inner(&mut self) -> W {
        self.writer.take().unwrap()
    }
}

impl<W> Sink<Packet> for PacketSink<W>
where W: AsyncWrite + Unpin + 'static
{
    type SinkError = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::SinkError>> {
        self.poll_flush(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Packet) -> Result<(), Self::SinkError> {
        let w = self.writer.take().unwrap();
        self.state = State::Sending(Box::pin(send(w, item)));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::SinkError>> {
        self.do_poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::SinkError>> {
        match &mut self.state {
            State::Ready => {
                let w = self.writer.take().unwrap();
                self.state = State::SendingGoodbye(Box::pin(send_goodbye(w)));
                self.poll_close(cx)
            },
            State::Sending(_) => match self.do_poll_flush(cx) {
                Pending => Pending,
                Ready(_) => {
                    self.state = State::Ready;
                    self.poll_close(cx)
                }
            },
            State::SendingGoodbye(fut) => {
                let p = Pin::as_mut(fut);

                match p.poll(cx) {
                    Ready((w, Ok(()))) => {
                        self.writer = Some(w);
                        self.state = State::Closing;
                        self.poll_close(cx)
                    },
                    Ready((w, Err(e))) => {
                        self.writer = Some(w);
                        Ready(Err(e))
                    },
                    Pending => Pending,
                }
            },
            State::Closing => {
                if let Some(ref mut w) = &mut self.writer {
                    Pin::new(w).poll_close(cx)
                } else {
                    panic!()
                }
            },
        }
    }
}
