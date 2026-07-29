use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tokio::sync::broadcast;

use crate::error::ServerError;

pub struct ShutdownHandle {
    pub workers: Vec<std::thread::JoinHandle<()>>,
    pub shutdown_tx: broadcast::Sender<()>,
    pub conn_count: Arc<AtomicUsize>,
    pub deadline_secs: u64,
}

impl ShutdownHandle {
    /// Broadcast shutdown → wait for workers to drain (or deadline) → join.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.shutdown_tx.send(());
        let deadline = Duration::from_secs(self.deadline_secs.max(1) + 1);
        let join = tokio::task::spawn_blocking(move || {
            for w in self.workers {
                let _ = w.join();
            }
        });
        match tokio::time::timeout(deadline, join).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("join workers: {e}"),
            ))),
            Err(_) => {
                tracing::warn!("shutdown deadline exceeded; workers may still be exiting");
                Ok(())
            }
        }
    }
}

pub async fn wait_for_signal() -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            r = signal::ctrl_c() => r?,
            _ = sigterm.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        Ok(())
    }
}
