use std::future::Future;
use std::pin::Pin;
use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle};

pub(crate) struct Scope<'a, T> {
    handle: Handle,
    task: Option<JoinHandle<T>>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<T> Drop for Scope<'_, T> {
    fn drop(&mut self) {
        if let Some(abort_handle) = self.task.take() {
            abort_handle.abort();
            let _ = self.handle.block_on(abort_handle);
        }
    }
}

impl<'a, T: Send + 'static> Scope<'a, T> {
    pub fn spawn<F>(handle: Handle, fut: F) -> Scope<'a, T>
    where
        F: Future<Output=T> + Send + 'a,
    {
        let task = handle.spawn(unsafe {
            // SAFETY: We erase `'a` to `'static` so Tokio can spawn the future.
            // This is sound because `Scope` guarantees the task cannot outlive `'a`:
            // `Drop` aborts the task and synchronously waits for it to finish,
            // ensuring the future is dropped before the scope ends. This is the
            // same scoped-task pattern used by the `async-scoped` crate.
            std::mem::transmute::<
                Pin<Box<dyn Future<Output=T> + Send + 'a>>,
                Pin<Box<dyn Future<Output=T> + Send>>,
            >(Box::pin(fut))
        });

        Scope {
            handle,
            task: Some(task),
            _marker: std::marker::PhantomData,
        }
    }

    #[allow(unused)]
    pub fn block_on(mut self) -> Result<T, JoinError> {
        let task = self.task.take().expect("task");
        self.handle.block_on(task)
    }

    pub fn finish_or_abort(mut self) -> Result<T, JoinError> {
        let task = self.task.take().expect("task");
        task.abort();
        self.handle.block_on(task)
    }
}
