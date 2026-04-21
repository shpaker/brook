//! Watch-task: подписывается на `BrookService::Watch` и шлёт события
//! в UI-mpsc.
//!
//! Реконнект — backoff 1s → 2s → 4s → … → 60s. При успешном
//! подключении шлём `UiEvent::StreamConnected` (UI чистит модель и
//! заливается из initial-Snapshot'ов, которые тут же приходят по
//! стриму). При ошибке стрима — `StreamDisconnected` и новая попытка.

use std::time::Duration;

use brook_proto::brook::v1::WatchRequest;
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::event::Kind as EventKind;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::events::{
    StreamEvent,
    UiEvent,
};

const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Запуск watch-task. Возвращает `JoinHandle`, абортом которого UI-task
/// глушит стрим при выходе.
pub fn spawn(channel: Channel, tx: mpsc::UnboundedSender<UiEvent>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { run(channel, tx).await })
}

async fn run(channel: Channel, tx: mpsc::UnboundedSender<UiEvent>) {
    let mut backoff = Duration::from_secs(1);
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut client = BrookServiceClient::new(channel.clone());
        match client.watch(WatchRequest {}).await {
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
                                && let Some(ue) = to_stream_event(kind)
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

fn to_stream_event(kind: EventKind) -> Option<StreamEvent> {
    match kind {
        EventKind::Snapshot(ev) => ev.download.map(StreamEvent::Snapshot),
        EventKind::Progress(ev) => Some(StreamEvent::Progress(ev.id?, ev.progress?)),
        EventKind::StateChanged(ev) => Some(StreamEvent::StateChanged(ev.id?, ev.state)),
        EventKind::WorkerUpdate(ev) => Some(StreamEvent::WorkerUpdate(
            ev.id?,
            ev.piece_index,
            ev.fraction,
        )),
        EventKind::Completed(ev) => Some(StreamEvent::Completed(ev.id?)),
        EventKind::Failed(ev) => Some(StreamEvent::Failed(ev.id?, ev.error)),
    }
}
