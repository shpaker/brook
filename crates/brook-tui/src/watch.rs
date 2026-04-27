//! Watch-tasks: две параллельные подписки на `BrookService::WatchFile`
//! и `BrookService::WatchProgress`, которые шлют события в UI-mpsc.
//!
//! Реконнект — backoff 1s → 2s → 4s → … → 60s. При успешном
//! подключении `WatchFile`-стрима шлём `UiEvent::StreamConnected` (UI
//! чистит модель и тут же запрашивает `GetRecently` — стрим больше не
//! делает initial-sync, отдаёт только дельты). `WatchProgress`
//! подписывается отдельно, без initial-sync, и молча переподключается
//! по той же схеме — коннект статус-бара отражает именно `WatchFile`.

use std::time::Duration;

use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::file_event::Kind as FileEventKind;
use brook_proto::brook::v1::{
    WatchFileRequest,
    WatchProgressRequest,
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
    let file_tx = tx.clone();
    let file_channel = channel.clone();
    let file = tokio::spawn(async move { run_file(file_channel, file_tx).await });
    let progress = tokio::spawn(async move { run_progress(channel, tx).await });
    (file, progress)
}

async fn run_file(channel: AuthedChannel, tx: mpsc::UnboundedSender<UiEvent>) {
    let mut backoff = Duration::from_secs(1);
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut client = BrookServiceClient::new(channel.clone());
        match client.watch_file(WatchFileRequest {}).await {
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
                            if let Some(kind) = ev.kind
                                && let Some(ue) = file_event_to_stream(kind)
                                && tx.send(UiEvent::Stream(Box::new(ue))).is_err()
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

fn file_event_to_stream(kind: FileEventKind) -> Option<StreamEvent> {
    match kind {
        FileEventKind::Created(ev) => ev.file.map(StreamEvent::Created),
        FileEventKind::Removed(ev) => ev.id.map(StreamEvent::Removed),
        FileEventKind::StatusChanged(ev) => Some(StreamEvent::StatusChanged(ev.id?, ev.status)),
        FileEventKind::Completed(ev) => Some(StreamEvent::Completed(ev.id?)),
        FileEventKind::Failed(ev) => Some(StreamEvent::Failed(ev.id?, ev.error)),
    }
}
