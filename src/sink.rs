use core::future::Future;
use core::pin::Pin;
use core::task::{Poll, Poll::Pending, Poll::Ready, Waker};
use futures::io::{AsyncWrite, AsyncWriteExt, Error};
use futures::sink::Sink;

use crate::packet::*;

type PinFut<O> = Pin<Box<dyn Future<Output=O> + 'static>>;

async fn send<W: AsyncWrite + 'static>(mut w: W, msg: Packet) -> (W, Result<(), Error>) {
    let h = msg.header();
    let mut r = await!(w.write_all(&h));
    if r.is_ok() {
        r = await!(w.write_all(&msg.body));
    }
    (w, r.map(|_| ()))
}


enum State<W> {
    Ready,
    Sending(PinFut<(W, Result<(), Error>)>),
    Closing,
}

pub struct PacketSink<W> {
    writer: Option<W>,
    state: State<W>
}
impl<W> PacketSink<W> {
    pub fn new(w: W) -> PacketSink<W> {
        PacketSink {
            writer: Some(w),
            state: State::Ready,
        }
    }

    fn do_poll_flush(&mut self, wk: &Waker) -> Poll<Result<(), Error>> {
        match &mut self.state {
            State::Ready => Ready(Ok(())),
            State::Sending(ref mut f) => {
                let p = Pin::as_mut(f);

                match p.poll(wk) {
                    Pending => Pending,
                    Ready((w, res)) => {
                        self.writer = Some(w);
                        self.state = State::Ready;
                        Ready(res)
                    }
                }
            },
            State::Closing => panic!() // TODO?
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
        self.state = State::Sending(Box::pin(send(w, item)));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, wk: &Waker) -> Poll<Result<(), Self::SinkError>> {
        self.do_poll_flush(wk)
    }

    fn poll_close(mut self: Pin<&mut Self>, wk: &Waker) -> Poll<Result<(), Self::SinkError>> {
        match self.state {
            State::Ready => {
                self.state = State::Closing;
                self.poll_close(wk)
            },
            State::Sending(_) => match self.do_poll_flush(wk) {
                Pending => Pending,
                Ready(_) => {
                    self.state = State::Closing;
                    self.poll_close(wk)
                }
            },
            State::Closing => {
                if let Some(ref mut w) = &mut self.writer {
                    w.poll_close(wk)
                } else {
                    panic!()
                }
            },
        }
    }
}
