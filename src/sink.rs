use core::pin::Pin;
use core::task::{Context, Poll, Poll::Pending, Poll::Ready};
use futures::io::{AsyncWrite, AsyncWriteExt};
use futures::sink::Sink;
use std::mem::replace;

// use crate::error::{Error, Error::*};
use crate::packet::*;
use crate::PinFut;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to send goodbye packet: {}", source))]
    SendGoodbye {
        source: std::io::Error
    },

    #[snafu(display("Failed to send packet: {}", source))]
    Send {
        source: std::io::Error
    },

    #[snafu(display("Failed to flush sink: {}", source))]
    Flush {
        source: std::io::Error
    },

    #[snafu(display("Error while closing sink: {}", source))]
    Close {
        source: std::io::Error
    },


}

async fn send<W>(mut w: W, msg: Packet) -> (W, Result<(), Error>)
where
    W: AsyncWrite + Unpin + 'static,
{
    let h = msg.header();
    let mut r = w.write_all(&h).await;
    if r.is_ok() {
        r = w.write_all(&msg.body).await;
    }
    (w, r.map(|_| ()).context(Send))
}

async fn send_goodbye<W>(mut w: W) -> (W, Result<(), Error>)
where
    W: AsyncWrite + Unpin + 'static,
{
    let r = w.write_all(&[0; 9]).await;
    (w, r.map(|_| ()).context(SendGoodbye{}))
}

/// #Examples
/// ```rust
/// #![feature(async_await)]
///
/// use std::io::Cursor;
/// use futures::executor::block_on;
/// use futures::prelude::SinkExt;
/// use ssb_packetstream::*;
///
/// let mut sink = PacketSink::new(Cursor::new(vec![0; 14]));
/// block_on(async {
///     sink.send(Packet::new(IsStream::Yes,
///                                  IsEnd::No,
///                                  BodyType::Json,
///                                  123,
///                                  vec![1,2,3,4,5])).await;
///     sink.close().await;
///     let buf = sink.into_inner().into_inner();
///     assert_eq!(&buf, &[0b0000_1010, 0, 0, 0, 5, 0, 0, 0, 123, 1, 2, 3, 4, 5]);
/// });
/// ```
pub struct PacketSink<W> {
    state: State<W>,
}
impl<W> PacketSink<W> {
    pub fn new(w: W) -> PacketSink<W> {
        PacketSink {
            state: State::Ready(w),
        }
    }

    pub fn into_inner(mut self) -> W {
        match self.state.take() {
            State::Ready(w) | State::Closing(w, _) | State::Closed(w) => w,
            _ => panic!(),
        }
    }
}

enum State<W> {
    Ready(W),
    Sending(PinFut<(W, Result<(), Error>)>),
    SendingGoodbye(PinFut<(W, Result<(), Error>)>),
    Closing(W, Option<Error>),
    Closed(W),
    Invalid,
}
impl<W> State<W> {
    fn take(&mut self) -> Self {
        replace(self, State::Invalid)
    }
}

fn flush<W>(state: State<W>, cx: &mut Context) -> (State<W>, Poll<Result<(), Error>>)
where
    W: AsyncWrite + Unpin + 'static,
{
    match state {
        State::Ready(mut w) => {
            let p = Pin::new(&mut w).poll_flush(cx).map(|r| r.context(Flush));
            (State::Ready(w), p)
        }
        State::Sending(mut f) => match f.as_mut().poll(cx) {
            Pending => (State::Sending(f), Pending),
            Ready((w, Err(e))) => close(State::Closing(w, Some(e)), cx),
            Ready((mut w, Ok(()))) => {
                let p = Pin::new(&mut w).poll_flush(cx).map(|r| r.context(Flush));
                (State::Ready(w), p)
            }
        },
        _ => panic!(), // TODO: can poll_flush be called after poll_close()?
    }
}

fn close<W>(state: State<W>, cx: &mut Context) -> (State<W>, Poll<Result<(), Error>>)
where
    W: AsyncWrite + Unpin + 'static,
{
    match state {
        State::Ready(w) => close(State::SendingGoodbye(Box::pin(send_goodbye(w))), cx),
        State::Sending(mut f) => match f.as_mut().poll(cx) {
            Pending => (State::Sending(f), Pending),
            Ready((w, Ok(()))) => close(State::SendingGoodbye(Box::pin(send_goodbye(w))), cx),
            Ready((w, Err(e))) => close(State::Closing(w, Some(e)), cx),
        },
        State::SendingGoodbye(mut f) => match f.as_mut().poll(cx) {
            Pending => (State::SendingGoodbye(f), Pending),
            Ready((w, Err(e))) => close(State::Closing(w, Some(e)), cx),
            Ready((mut w, Ok(()))) => {
                let p = Pin::new(&mut w).poll_close(cx).map(|r| r.context(Close));
                (State::Closing(w, None), p)
            }
        },
        State::Closing(mut w, e) => {
            match (Pin::new(&mut w).poll_close(cx).map(|r| r.context(Close)), e) {
                (Pending, e) => (State::Closing(w, e), Pending),
                (Ready(r), None) => (State::Closed(w), Ready(r)),
                (Ready(_), Some(e)) => (State::Closed(w), Ready(Err(e))), // Combine errors if this fails?
            }
        }

        st @ State::Closed(_) => (st, Ready(Ok(()))),
        _ => panic!(),
    }
}

impl<W> Sink<Packet> for PacketSink<W>
where
    W: AsyncWrite + Unpin + 'static,
{
    type SinkError = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::SinkError>> {
        self.poll_flush(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Packet) -> Result<(), Self::SinkError> {
        match self.state.take() {
            State::Ready(w) => {
                self.state = State::Sending(Box::pin(send(w, item)));
                Ok(())
            }
            _ => panic!(),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::SinkError>> {
        let (state, poll) = flush(self.state.take(), cx);
        self.state = state;
        poll
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::SinkError>> {
        let (state, poll) = close(self.state.take(), cx);
        self.state = state;
        poll
    }
}
