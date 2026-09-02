//! Process-runtime ownership for I/O reached through Actix's local workers.
//!
//! Request futures remain on the worker's `LocalSet`, but pooled connections,
//! timers, and Tokio child tasks must outlive that worker during staged drain.
//! Both polling and destruction therefore enter the process runtime. Entering
//! only while constructing a future, or only while polling it, is insufficient:
//! cancellation can return a `SQLx` connection and spawn pool maintenance in Drop.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use actix_web::{
    Error,
    body::{BodySize, MessageBody},
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
    web::Bytes,
};
use tokio::runtime::Handle;

/// Own a pinned request, response body, or local session until its process-bound
/// cleanup completes. The composition root must keep `runtime` alive throughout
/// HTTP shutdown and every later drain stage.
pub struct RuntimeScope<T> {
    value: Option<Pin<Box<T>>>,
    runtime: Handle,
}

impl<T> RuntimeScope<T> {
    pub fn new(value: T, runtime: Handle) -> Self {
        Self {
            value: Some(Box::pin(value)),
            runtime,
        }
    }
}

impl<T: Future> Future for RuntimeScope<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _entered = this.runtime.enter();
        // Completed futures are fused; their owned state was already
        // destroyed under the process runtime when they returned Ready.
        let result = this
            .value
            .as_mut()
            .map_or(Poll::Pending, |value| value.as_mut().poll(context));
        if result.is_ready() {
            drop(this.value.take());
        }
        result
    }
}

impl<T: MessageBody> MessageBody for RuntimeScope<T> {
    type Error = T::Error;

    fn size(&self) -> BodySize {
        let _entered = self.runtime.enter();
        self.value
            .as_ref()
            .map_or(BodySize::None, |value| value.as_ref().get_ref().size())
    }

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        let this = self.get_mut();
        let _entered = this.runtime.enter();
        let result = this
            .value
            .as_mut()
            .map_or(Poll::Ready(None), |value| value.as_mut().poll_next(context));
        if matches!(result, Poll::Ready(None)) {
            drop(this.value.take());
        }
        result
    }
}

impl<T> Drop for RuntimeScope<T> {
    fn drop(&mut self) {
        let _entered = self.runtime.enter();
        drop(self.value.take());
    }
}

pub async fn request_runtime<B: MessageBody>(
    request: ServiceRequest,
    next: Next<B>,
    runtime: Handle,
) -> Result<ServiceResponse<RuntimeScope<B>>, Error> {
    // Calling the inner service can itself create I/O. Defer that call into
    // the scoped future, then preserve the same owner for a streaming body.
    let response =
        RuntimeScope::new(async move { next.call(request).await }, runtime.clone()).await?;
    Ok(response.map_body(move |_head, body| RuntimeScope::new(body, runtime)))
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        convert::Infallible,
        error::Error as StdError,
        future::{self, poll_fn},
        pin::Pin,
        rc::Rc,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        thread,
        time::Duration,
    };

    use actix_web::{
        body::{BodySize, MessageBody},
        rt,
        web::Bytes,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        runtime::{Builder, Handle, Id},
        task::LocalSet,
    };

    use super::RuntimeScope;

    type TestResult = Result<(), Box<dyn StdError>>;

    struct RuntimeProbe {
        observations: Arc<Mutex<Vec<(&'static str, Id)>>>,
    }

    impl RuntimeProbe {
        fn observe(&self, phase: &'static str) {
            self.observations
                .lock()
                .expect("runtime observation lock")
                .push((phase, Handle::current().id()));
        }
    }

    impl Drop for RuntimeProbe {
        fn drop(&mut self) {
            self.observe("drop");
        }
    }

    impl MessageBody for RuntimeProbe {
        type Error = Infallible;

        fn size(&self) -> BodySize {
            self.observe("size");
            BodySize::Stream
        }

        fn poll_next(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Bytes, Self::Error>>> {
            self.observe("body");
            Poll::Ready(None)
        }
    }

    struct PendingBody {
        probe: RuntimeProbe,
    }

    impl MessageBody for PendingBody {
        type Error = Infallible;

        fn size(&self) -> BodySize {
            self.probe.observe("size");
            BodySize::Stream
        }

        fn poll_next(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Bytes, Self::Error>>> {
            self.probe.observe("pending");
            Poll::Pending
        }
    }

    #[test]
    fn local_session_preserves_thread() -> TestResult {
        let owner = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let caller = Builder::new_current_thread().enable_all().build()?;
        let local = LocalSet::new();
        let caller_thread = thread::current().id();
        let owner_id = owner.handle().id();
        let value = Rc::new(Cell::new(0));
        let session_value = Rc::clone(&value);
        let session = async move {
            assert_eq!(thread::current().id(), caller_thread);
            assert_eq!(Handle::current().id(), owner_id);
            session_value.set(1);
            tokio::task::yield_now().await;
            assert_eq!(thread::current().id(), caller_thread);
            assert_eq!(Handle::current().id(), owner_id);
            session_value.set(session_value.get() + 1);
        };
        local.block_on(
            &caller,
            RuntimeScope::new(
                async move { rt::spawn(RuntimeScope::new(session, Handle::current())).await },
                owner.handle().clone(),
            ),
        )?;
        assert_eq!(value.get(), 2);
        Ok(())
    }

    #[test]
    fn pending_body_uses_owner() -> TestResult {
        let owner = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let caller = Builder::new_current_thread().enable_all().build()?;
        let observations = Arc::new(Mutex::new(Vec::new()));
        let body = PendingBody {
            probe: RuntimeProbe {
                observations: Arc::clone(&observations),
            },
        };
        caller.block_on(async {
            let mut body = RuntimeScope::new(body, owner.handle().clone());
            assert_eq!(body.size(), BodySize::Stream);
            let first_poll =
                poll_fn(|context| Poll::Ready(Pin::new(&mut body).poll_next(context))).await;
            assert!(first_poll.is_pending());
            assert_eq!(Handle::current().id(), caller.handle().id());
            drop(body);
        });
        assert_eq!(
            *observations.lock().expect("runtime observation lock"),
            vec![
                ("size", owner.handle().id()),
                ("pending", owner.handle().id()),
                ("drop", owner.handle().id()),
            ]
        );
        Ok(())
    }

    #[test]
    fn future_completion_uses_owner() -> TestResult {
        let owner = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let caller = Builder::new_current_thread().enable_all().build()?;
        let observations = Arc::new(Mutex::new(Vec::new()));
        let probe = RuntimeProbe {
            observations: Arc::clone(&observations),
        };
        let task = async move {
            probe.observe("first_poll");
            tokio::task::yield_now().await;
            probe.observe("second_poll");
            let child = tokio::spawn(async { Handle::current().id() });
            child.await
        };
        let child_owner = caller.block_on(RuntimeScope::new(task, owner.handle().clone()))?;
        assert_eq!(child_owner, owner.handle().id());
        assert_eq!(
            *observations.lock().expect("runtime observation lock"),
            vec![
                ("first_poll", owner.handle().id()),
                ("second_poll", owner.handle().id()),
                ("drop", owner.handle().id()),
            ]
        );
        Ok(())
    }

    #[test]
    fn cancellation_uses_owner() -> TestResult {
        let owner = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let caller = Builder::new_current_thread().enable_all().build()?;
        let observations = Arc::new(Mutex::new(Vec::new()));
        let probe = RuntimeProbe {
            observations: Arc::clone(&observations),
        };
        let task = async move {
            probe.observe("poll");
            future::pending::<()>().await;
        };
        caller.block_on(async {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(5),
                    RuntimeScope::new(task, owner.handle().clone()),
                )
                .await
                .is_err()
            );
        });
        assert_eq!(
            *observations.lock().expect("runtime observation lock"),
            vec![("poll", owner.handle().id()), ("drop", owner.handle().id())]
        );
        Ok(())
    }

    #[test]
    fn body_lifecycle_uses_owner() -> TestResult {
        let owner = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let caller = Builder::new_current_thread().enable_all().build()?;
        let observations = Arc::new(Mutex::new(Vec::new()));
        for consume in [false, true] {
            let probe = RuntimeProbe {
                observations: Arc::clone(&observations),
            };
            caller.block_on(async {
                let mut body = RuntimeScope::new(probe, owner.handle().clone());
                assert_eq!(body.size(), BodySize::Stream);
                if consume {
                    assert!(
                        poll_fn(|context| Pin::new(&mut body).poll_next(context))
                            .await
                            .is_none()
                    );
                }
                drop(body);
            });
        }
        assert_eq!(
            *observations.lock().expect("runtime observation lock"),
            vec![
                ("size", owner.handle().id()),
                ("drop", owner.handle().id()),
                ("size", owner.handle().id()),
                ("body", owner.handle().id()),
                ("drop", owner.handle().id()),
            ]
        );
        Ok(())
    }

    #[test]
    fn connection_outlives_worker() -> TestResult {
        let owner = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let caller = Builder::new_current_thread().enable_all().build()?;
        let listener = owner.block_on(TcpListener::bind("127.0.0.1:0"))?;
        let address = listener.local_addr()?;
        let accepted = owner.spawn(async move { listener.accept().await });
        let mut client = caller.block_on(RuntimeScope::new(
            TcpStream::connect(address),
            owner.handle().clone(),
        ))?;
        drop(caller);
        owner.block_on(async move {
            let (mut server, _) = accepted.await??;
            client.write_all(b"drain").await?;
            let mut message = [0_u8; 5];
            tokio::time::timeout(Duration::from_secs(1), server.read_exact(&mut message)).await??;
            assert_eq!(&message, b"drain");
            server.write_all(b"ack").await?;
            let mut acknowledgment = [0_u8; 3];
            tokio::time::timeout(
                Duration::from_secs(1),
                client.read_exact(&mut acknowledgment),
            )
            .await??;
            assert_eq!(&acknowledgment, b"ack");
            Ok(())
        })
    }
}
