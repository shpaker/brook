//! Запуск unary-команд (`Add`, `Pause`, `Resume`, `Remove`) из UI-task'а.
//! Каждая команда крутится в `tokio::spawn`, результат возвращается в UI
//! как `UiEvent::CmdResult` — ошибки показываются toast'ом, `FileExists`
//! на `Add` поднимает модалку выбора политики.

use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::{
    AddRequest,
    DownloadId,
    DownloadSpec,
    IdRequest,
    OnFileExistsOverride,
    RemoveRequest,
    ShutdownRequest,
};
use tokio::sync::mpsc::UnboundedSender;
use tonic::transport::Channel;
use tonic::{
    Code,
    Status,
};

use crate::events::{
    AddForm,
    CmdOutcome,
    UiEvent,
};

/// Отличаем `FileExists` от прочих серверных ошибок по префиксу
/// сообщения. `brook-api` мапит `PrepareError::FileExists` именно так
/// (см. [`crates/brook-api/src/mapper.rs`](../brook-api/src/mapper.rs)).
fn is_file_exists(st: &Status) -> bool {
    st.code() == Code::AlreadyExists
        || (st.code() == Code::FailedPrecondition && st.message().to_lowercase().contains("exists"))
}

fn send(tx: &UnboundedSender<UiEvent>, outcome: CmdOutcome) {
    let _ = tx.send(UiEvent::CmdResult(outcome));
}

pub fn add(
    ch: Channel,
    tx: UnboundedSender<UiEvent>,
    form: AddForm,
    on_file_exists_override: OnFileExistsOverride,
) {
    tokio::spawn(async move {
        let mut client = BrookServiceClient::new(ch);
        let spec = DownloadSpec {
            url: form.url.clone(),
            target_dir: form.folder.clone(),
            on_file_exists_override: on_file_exists_override as i32,
            ..Default::default()
        };
        match client.add(AddRequest { spec: Some(spec) }).await {
            Ok(_) => send(&tx, CmdOutcome::AddAccepted),
            Err(st)
                if is_file_exists(&st)
                    && on_file_exists_override == OnFileExistsOverride::Unspecified =>
            {
                send(&tx, CmdOutcome::AddFileExists { form });
            }
            Err(st) => send(
                &tx,
                CmdOutcome::Error(format!("add failed: {}", st.message())),
            ),
        }
    });
}

fn id_request(id: &str) -> IdRequest {
    IdRequest {
        id: Some(DownloadId { value: id.into() }),
    }
}

pub fn pause(ch: Channel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    run_bulk(ch, tx, ids, "pause", |mut client, req| {
        Box::pin(async move { client.pause(req).await.map(|_| ()) })
    });
}

pub fn resume(ch: Channel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    run_bulk(ch, tx, ids, "resume", |mut client, req| {
        Box::pin(async move { client.resume(req).await.map(|_| ()) })
    });
}

/// Remove идемпотентен на стороне демона (см. `manager::remove`): ghost
/// id даст `Ok`. Успешно обработанные id отдаём UI, чтобы тот дропнул
/// их из ViewModel — событий по удалению демон не генерит.
pub fn remove(ch: Channel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    tokio::spawn(async move {
        let mut removed: Vec<String> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        for id in &ids {
            let mut client = BrookServiceClient::new(ch.clone());
            let req = RemoveRequest {
                id: Some(DownloadId { value: id.clone() }),
            };
            match client.remove(req).await {
                Ok(_) => removed.push(id.clone()),
                Err(st) => errors.push(format!("{}: {}", short_id(id), st.message())),
            }
        }
        if !removed.is_empty() {
            send(&tx, CmdOutcome::Removed { ids: removed });
        }
        if !errors.is_empty() {
            send(
                &tx,
                CmdOutcome::Error(format!("delete failed — {}", errors.join("; "))),
            );
        } else if ids.is_empty() {
            send(&tx, CmdOutcome::Ok);
        }
    });
}

type BulkFn = fn(
    BrookServiceClient<Channel>,
    IdRequest,
)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Status>> + Send>>;

fn run_bulk(
    ch: Channel,
    tx: UnboundedSender<UiEvent>,
    ids: Vec<String>,
    op: &'static str,
    call: BulkFn,
) {
    tokio::spawn(async move {
        let mut not_found: Vec<String> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        for id in &ids {
            let client = BrookServiceClient::new(ch.clone());
            let req = id_request(id);
            if let Err(st) = call(client, req).await {
                if st.code() == Code::NotFound {
                    not_found.push(id.clone());
                } else {
                    errors.push(format!("{}: {}", short_id(id), st.message()));
                }
            }
        }
        let had_not_found = !not_found.is_empty();
        if had_not_found {
            send(&tx, CmdOutcome::NotFound { ids: not_found });
        }
        if !errors.is_empty() {
            send(
                &tx,
                CmdOutcome::Error(format!("{op} failed — {}", errors.join("; "))),
            );
        } else if !had_not_found {
            send(&tx, CmdOutcome::Ok);
        }
    });
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

/// Послать `Shutdown` RPC и по завершении (успех или ошибка) отправить
/// `UiEvent::Quit`. Ошибки игнорируем намеренно: цель — выйти из TUI;
/// если демон не отозвался, висеть в модалке смысла нет.
pub fn shutdown_daemon(ch: Channel, tx: UnboundedSender<UiEvent>) {
    tokio::spawn(async move {
        let mut client = BrookServiceClient::new(ch);
        let _ = client.shutdown(ShutdownRequest {}).await;
        let _ = tx.send(UiEvent::Quit);
    });
}

/// Попытка взять URL из clipboard — blocking, поэтому крутим через
/// `spawn_blocking`. Ошибки глотаем: префилл clipboard — удобство, а
/// не требование.
pub fn clipboard_url() -> Option<String> {
    let mut cb = arboard::Clipboard::new().ok()?;
    let text = cb.get_text().ok()?;
    let t = text.trim();
    if t.starts_with("http://") || t.starts_with("https://") {
        Some(t.to_string())
    } else {
        None
    }
}
