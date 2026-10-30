//! openraft on the madsim simulator (`--cfg madsim`): spawn, sleep and
//! timeouts from the simulator; tokio's sync primitives and `Instant` work
//! as they are (the simulator answers the clock).

use openraft::async_runtime::TokioOneShotSender;
use openraft::{AsyncRuntime, OptionalSend, TokioInstant};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct MadsimRuntime;

impl AsyncRuntime for MadsimRuntime {
    type JoinError = madsim::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = madsim::task::JoinHandle<T>;
    type Sleep = madsim::time::Sleep;
    type Instant = TokioInstant;
    type TimeoutError = madsim::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = Timeout<T>;
    type ThreadLocalRng = madsim::rand::GlobalRng;
    type OneshotSender<T: OptionalSend> = TokioOneShotSender<T>;
    type OneshotReceiverError = tokio::sync::oneshot::error::RecvError;
    type OneshotReceiver<T: OptionalSend> = tokio::sync::oneshot::Receiver<T>;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        madsim::task::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        madsim::time::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        madsim::time::sleep_until(deadline.into_std())
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        Timeout { future: Box::pin(future), sleep: Box::pin(madsim::time::sleep(duration)) }
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        Timeout { future: Box::pin(future), sleep: Box::pin(madsim::time::sleep_until(deadline.into_std())) }
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        madsim::rand::thread_rng()
    }

    fn oneshot<T>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>)
    where
        T: OptionalSend,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (TokioOneShotSender(tx), rx)
    }
}

/// A future that gives up once its sleep ends first.
pub struct Timeout<F> {
    future: Pin<Box<F>>,
    sleep: Pin<Box<madsim::time::Sleep>>,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, madsim::time::error::Elapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = self.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(madsim::time::error::Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}
