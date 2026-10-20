//! Serialized owner futures can also run at their input's admission boundary.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use tokio::task::JoinHandle;

mod handoff;
use handoff::Handoff;

type Owner = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Retains one future and coalesces wakes while its caller is already driving it.
pub(crate) struct Task {
    owner: Mutex<Option<Owner>>,
    scheduled: Mutex<Option<Waker>>,
    handoff: Handoff,
}

impl Task {
    /// Holds the real owner outside polling so admission tests can exercise contention.
    #[cfg(test)]
    pub(crate) fn hold(&self) -> std::sync::MutexGuard<'_, Option<Owner>> {
        self.owner.lock().unwrap()
    }

    /// Starts the ordinary Tokio owner and exposes only a weak inline capability.
    pub(crate) fn spawn<F: Future<Output = ()> + Send + 'static>(
        make: impl FnOnce(Weak<Self>) -> F,
    ) -> (JoinHandle<()>, Weak<Self>) {
        let task = Arc::new(Self {
            owner: Mutex::new(None),
            scheduled: Mutex::new(None),
            handoff: Handoff::default(),
        });
        let weak = Arc::downgrade(&task);
        *task.owner.lock().unwrap() = Some(Box::pin(make(weak.clone())));
        (tokio::spawn(Runner(task)), weak)
    }

    /// Commits input and polls its existing owner without a task hop when idle.
    pub(crate) fn drive<T>(weak: &Weak<Self>, commit: impl FnOnce() -> T) -> T {
        Self::dispatch(weak, |_| (commit(), true))
    }

    /// Allows an idle owner's synchronous write to finish without polling its input loop.
    /// The callback reports whether queued work still needs that original loop.
    pub(crate) fn dispatch<T>(weak: &Weak<Self>, commit: impl FnOnce(bool) -> (T, bool)) -> T {
        let Some(task) = weak.upgrade() else {
            return commit(false).0;
        };
        let Ok(mut owner) = task.owner.try_lock() else {
            return commit(false).0;
        };
        task.handoff.enter();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let (result, poll) = commit(owner.is_some());
            if poll {
                for _ in 0..4 {
                    task.handoff.prepare();
                    if task.poll_owner(&mut owner).is_ready() || !task.handoff.notified() {
                        break;
                    }
                }
            }
            result
        }));
        if result.is_err() {
            *owner = None;
            task.handoff.wake();
        }
        let wake = task.handoff.leave();
        drop(owner);
        if wake {
            task.schedule();
        }
        result.unwrap_or_else(|panic| resume_unwind(panic))
    }

    /// Polls the same pinned future with a waker independent of the borrowing caller.
    fn poll_owner(self: &Arc<Self>, owner: &mut Option<Owner>) -> Poll<()> {
        let Some(future) = owner else {
            return Poll::Ready(());
        };
        let waker = Waker::from(Arc::clone(self));
        let result = future.as_mut().poll(&mut Context::from_waker(&waker));
        if result.is_ready() {
            *owner = None;
            self.handoff.wake();
        }
        result
    }

    /// Wakes the original Tokio task after an event, contention or inline completion.
    fn schedule(&self) {
        if let Some(waker) = self.scheduled.lock().unwrap().as_ref() {
            waker.wake_by_ref();
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.handoff.wake() {
            self.schedule();
        }
    }
}

/// Aborting the original task still destroys its protocol owner and admission guard.
struct Runner(Arc<Task>);

impl Future for Runner {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        *self.0.scheduled.lock().unwrap() = Some(context.waker().clone());
        let Ok(mut owner) = self.0.owner.try_lock() else {
            self.0.wake_by_ref();
            return Poll::Pending;
        };
        self.0.handoff.prepare();
        let result = catch_unwind(AssertUnwindSafe(|| self.0.poll_owner(&mut owner)));
        if result.is_err() {
            *owner = None;
        }
        drop(owner);
        result.unwrap_or_else(|panic| resume_unwind(panic))
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.0.owner.lock().unwrap().take();
        self.0.scheduled.lock().unwrap().take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::oneshot;

    /// Records destruction of the original owner, including a future not polled yet.
    struct Retired(Arc<AtomicBool>);
    impl Drop for Retired {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn readiness_after_inline_pending_resumes_the_original_task() {
        let (first, input) = oneshot::channel();
        let (last, finish) = oneshot::channel();
        let (runner, task) = Task::spawn(|_| async move {
            input.await.unwrap();
            finish.await.unwrap();
        });
        tokio::task::yield_now().await;
        Task::drive(&task, || first.send(()).unwrap());
        last.send(()).unwrap();
        runner.await.unwrap();
        assert!(task.upgrade().is_none());
    }

    #[tokio::test]
    async fn a_direct_write_cannot_consume_unpolled_readiness() {
        let (send, receive) = oneshot::channel();
        let (runner, task) = Task::spawn(|_| async move {
            receive.await.unwrap();
        });
        tokio::task::yield_now().await;
        Task::dispatch(&task, |exclusive| {
            assert!(exclusive);
            send.send(()).unwrap();
            ((), false)
        });
        runner.await.unwrap();
        assert!(task.upgrade().is_none());
    }

    #[tokio::test]
    async fn contended_admission_and_abort_release_the_original_owner() {
        let retired = Arc::new(AtomicBool::new(false));
        let guard = Retired(Arc::clone(&retired));
        let (send, receive) = oneshot::channel();
        let (runner, task) = Task::spawn(|_| async move {
            let _guard = guard;
            receive.await.unwrap();
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        {
            let original = task.upgrade().unwrap();
            let _held = original.hold();
            Task::drive(&task, || send.send(()).unwrap());
        }
        tokio::task::yield_now().await;
        runner.abort();
        assert!(runner.await.unwrap_err().is_cancelled());
        assert!(retired.load(Ordering::Acquire));
        assert!(task.upgrade().is_none());
    }

    #[tokio::test]
    async fn inline_panic_retires_before_unwinding_into_the_caller() {
        let retired = Arc::new(AtomicBool::new(false));
        let guard = Retired(Arc::clone(&retired));
        let (send, receive) = oneshot::channel();
        let (runner, task) = Task::spawn(|_| async move {
            let _guard = guard;
            receive.await.unwrap();
            panic!("original owner failed");
        });
        let result = catch_unwind(AssertUnwindSafe(|| {
            Task::drive(&task, || send.send(()).unwrap());
        }));
        assert!(result.is_err());
        assert!(retired.load(Ordering::Acquire));
        runner.await.unwrap();
        assert!(task.upgrade().is_none());
    }
}
