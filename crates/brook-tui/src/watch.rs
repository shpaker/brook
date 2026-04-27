//! Watch-tasks: две параллельные подписки на `BrookService::WatchStatus`
//! и `BrookService::WatchProgress`, которые шлют события в UI-mpsc.
//!
//! Реконнект — backoff 1s → 2s → 4s → … → 60s. При успешном
//! подключении `WatchStatus`-стрима шлём `UiEvent::StreamConnected` (UI
//! чистит модель и тут же запрашивает `GetRecently` — стрим не делает
//! initial-sync, отдаёт только статусные переходы). `WatchProgress`
//! подписывается отдельно, без initial-sync, и молча переподключается
//! по той же схеме — коннект статус-бара отражает именно `WatchStatus`.

use std::time::Duration;

use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::{
    WatchProgressRequest,
    WatchStatusRequest,
};
use tokio::sync::mpsc;

use crate::connect::AuthedChannel;
use crate::events::{
    StreamEvent,
    UiEvent,
};

const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Запускает обе watch-задачи. Возвращает пару `JoinHandle`, абортом
/// которых UI-task глушит стримы при выходе.
pub fn spawn(
    channel: AuthedChannel,
    tx: mpsc::UnboundedSender<UiEvent>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let status_tx = tx.clone();
    let status_channel = channel.clone();
    let status = tokio::spawn(async move { run_status(status_channel, status_tx).await });
    let progress = tokio::spawn(async move { run_progress(channel, tx).await });
    (status, progress)
}

async fn run_status(channel: AuthedChannel, tx: mpsc::UnboundedSender<UiEvent>) {
    let mut backoff = Duration::from_secs(1);
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut client = BrookServiceClient::new(channel.clone());
        match client.watch_status(WatchStatusRequest {}).await {
            Ok(resp) => {
                attempt = 0;
                backoff = Duration::from_secs(1);
                if tx.send(UiEvent::StreamConnected).is_err() {
                    return;
                }
                let mut stream = resp.into_inner();
                loop {
                    match stream.message().await {
                        Ok(Some(ev)) => {
                            if tx
                                .send(UiEvent::Stream(Box::new(StreamEvent::Status(ev))))
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(None) => {
                            let _ =
                                tx.send(UiEvent::StreamDisconnected("server closed stream".into()));
                            break;
                        }
                        Err(e) => {
                            let _ =
                                tx.send(UiEvent::StreamDisconnected(format!("stream error: {e}")));
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(UiEvent::StreamDisconnected(format!(
                    "connect failed (attempt {attempt}): {e}"
                )));
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn run_progress(channel: AuthedChannel, tx: mpsc::UnboundedSender<UiEvent>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut client = BrookServiceClient::new(channel.clone());
        if let Ok(resp) = client.watch_progress(WatchProgressRequest {}).await {
            backoff = Duration::from_secs(1);
            let mut stream = resp.into_inner();
            while let Ok(Some(tick)) = stream.message().await {
                if tx
                    .send(UiEvent::Stream(Box::new(StreamEvent::Progress(tick))))
                    .is_err()
                {
                    return;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}
