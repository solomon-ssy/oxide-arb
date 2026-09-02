//! Test-only HTTP peer that accepts connections and never responds.

use std::future::pending;

use tokio::{net::TcpListener, task::JoinHandle};

pub(super) struct NeverResponseServer {
    url: String,
    task: JoinHandle<()>,
}

impl NeverResponseServer {
    pub(super) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind never-response ClickHouse peer");
        let address = listener
            .local_addr()
            .expect("never-response ClickHouse peer address");
        let task = tokio::spawn(async move {
            while let Ok((connection, _)) = listener.accept().await {
                drop(tokio::spawn(async move {
                    pending::<()>().await;
                    drop(connection);
                }));
            }
        });
        Self {
            url: format!("http://{address}"),
            task,
        }
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for NeverResponseServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
