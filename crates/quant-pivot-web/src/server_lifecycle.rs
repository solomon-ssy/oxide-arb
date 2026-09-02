//! Keep the Actix command driver alive until its worker-stop ACK completes.

use actix_web::dev::Server;
use quant_pivot_error::{QuantResult, infra::InfraError};
use tokio_util::sync::CancellationToken;

pub struct ServerLifecycle {
    pub server: Server,
    pub shutdown: CancellationToken,
}

impl ServerLifecycle {
    pub async fn run(mut self) -> QuantResult<()> {
        let handle = self.server.handle();
        let result = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => {
                // Server owns the command receiver. Dropping it before stop,
                // or waiting only for the ACK without polling Server, cannot
                // drain workers. Keep driving both through final completion.
                let (result, ()) = tokio::join!(&mut self.server, handle.stop(true));
                result
            }
            result = &mut self.server => result,
        };
        result.map_err(|error| {
            InfraError::ServerRuntime {
                detail: error.to_string(),
            }
            .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use actix_web::{
        App, HttpResponse, HttpServer,
        web::{self, Data},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::Notify,
    };
    use tokio_util::sync::CancellationToken;

    use super::ServerLifecycle;

    struct PendingRequest {
        entered: Notify,
        release: Notify,
        completed: AtomicBool,
    }

    async fn held_request(state: Data<PendingRequest>) -> HttpResponse {
        state.entered.notify_one();
        state.release.notified().await;
        state.completed.store(true, Ordering::Release);
        HttpResponse::Ok().body("drained")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_drains_inflight() -> Result<(), Box<dyn StdError>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let state = Arc::new(PendingRequest {
            entered: Notify::new(),
            release: Notify::new(),
            completed: AtomicBool::new(false),
        });
        let request_state = Arc::clone(&state);
        let server = HttpServer::new(move || {
            App::new()
                .app_data(Data::from(Arc::clone(&request_state)))
                .route("/", web::get().to(held_request))
        })
        .workers(1)
        .worker_max_blocking_threads(1)
        .disable_signals()
        .shutdown_timeout(1)
        .listen(listener)?
        .run();
        let shutdown = CancellationToken::new();
        let lifecycle = ServerLifecycle {
            server,
            shutdown: shutdown.clone(),
        };
        let mut server_task = tokio::spawn(lifecycle.run());
        let mut request = TcpStream::connect(address).await?;
        request
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        tokio::time::timeout(Duration::from_secs(2), state.entered.notified()).await?;
        shutdown.cancel();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut server_task)
                .await
                .is_err(),
            "stop must wait for the in-flight response"
        );
        state.release.notify_one();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), request.read_to_end(&mut response)).await??;
        tokio::time::timeout(Duration::from_secs(2), server_task).await???;
        assert!(state.completed.load(Ordering::Acquire));
        assert!(response.ends_with(b"drained"));
        assert!(TcpStream::connect(address).await.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grace_bounds_slow_requests() -> Result<(), Box<dyn StdError>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let state = Arc::new(PendingRequest {
            entered: Notify::new(),
            release: Notify::new(),
            completed: AtomicBool::new(false),
        });
        let request_state = Arc::clone(&state);
        let server = HttpServer::new(move || {
            App::new()
                .app_data(Data::from(Arc::clone(&request_state)))
                .route("/", web::get().to(held_request))
        })
        .workers(1)
        .worker_max_blocking_threads(1)
        .disable_signals()
        .shutdown_timeout(1)
        .listen(listener)?
        .run();
        let shutdown = CancellationToken::new();
        let lifecycle = ServerLifecycle {
            server,
            shutdown: shutdown.clone(),
        };
        let server_task = tokio::spawn(lifecycle.run());
        let mut request = TcpStream::connect(address).await?;
        request
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        tokio::time::timeout(Duration::from_secs(2), state.entered.notified()).await?;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(3), server_task).await???;
        assert!(!state.completed.load(Ordering::Acquire));
        assert!(TcpStream::connect(address).await.is_err());
        Ok(())
    }
}
