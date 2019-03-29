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
