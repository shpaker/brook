//! Запуск unary-команд (`Add`, `Pause`, `Resume`, `Cancel`, `Remove`)
//! из UI-task'а. Каждая команда крутится в `tokio::spawn`, результат
//! возвращается в UI как `UiEvent::CmdResult` — ошибки показываются
//! toast'ом, `FileExists` на `Add` поднимает модалку выбора политики.

use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::{
    AddRequest,
    DownloadId,
    DownloadSpec,
    IdRequest,
    OnFileExistsOverride,
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

pub fn cancel(ch: Channel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    run_bulk(ch, tx, ids, "cancel", |mut client, req| {
        Box::pin(async move { client.cancel(req).await.map(|_| ()) })
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
        let mut errors: Vec<String> = Vec::new();
        for id in &ids {
            let client = BrookServiceClient::new(ch.clone());
            let req = id_request(id);
            if let Err(st) = call(client, req).await {
                errors.push(format!("{}: {}", short_id(id), st.message()));
            }
        }
        if errors.is_empty() {
            send(&tx, CmdOutcome::Ok);
        } else {
            send(
                &tx,
                CmdOutcome::Error(format!("{op} failed — {}", errors.join("; "))),
            );
        }
    });
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

/// Открытие файла/папки — `open` на macOS. Ошибки не критичны, их
/// отдаём toast'ом.
pub fn open_path(tx: UnboundedSender<UiEvent>, path: String) {
    tokio::spawn(async move {
        match tokio::process::Command::new("open")
            .arg(&path)
            .status()
            .await
        {
            Ok(s) if s.success() => send(&tx, CmdOutcome::Ok),
            Ok(s) => send(&tx, CmdOutcome::Error(format!("open exited with {s}"))),
            Err(e) => send(&tx, CmdOutcome::Error(format!("open failed: {e}"))),
        }
    });
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
