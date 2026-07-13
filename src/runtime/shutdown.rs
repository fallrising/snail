use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use tokio::signal;
use tokio::sync::broadcast;

use crate::error::ServerError;

pub struct ShutdownHandle {
    pub workers: Vec<std::thread::JoinHandle<()>>,
    pub shutdown_tx: broadcast::Sender<()>,
    pub conn_count: Arc<AtomicUsize>,
}

impl ShutdownHandle {
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.shutdown_tx.send(());
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        for w in self.workers {
            let _ = w.join();
        }
        Ok(())
    }
}

pub async fn wait_for_signal() -> Result<(), ServerError> {
    signal::ctrl_c().await?;
    Ok(())
}