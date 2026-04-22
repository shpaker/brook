//! Запуск unary-команд (`Add`, `Pause`, `Resume`, `Remove`) из UI-task'а.
//! Каждая команда крутится в `tokio::spawn`, результат возвращается в UI
//! как `UiEvent::CmdResult` — ошибки показываются toast'ом.

use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use brook_proto::brook::v1::{
    AddRequest,
    FileId,
    FileSpec,
    IdRequest,
    RemoveRequest,
    ShutdownRequest,
};
use tokio::sync::mpsc::UnboundedSender;
use tonic::{
    Code,
    Status,
};

use crate::connect::AuthedChannel;
use crate::events::{
    AddForm,
    CmdOutcome,
    UiEvent,
};

fn send(tx: &UnboundedSender<UiEvent>, outcome: CmdOutcome) {
    let _ = tx.send(UiEvent::CmdResult(outcome));
}

/// Послать `Add`. Если демон вернул `AlreadyExists`, возвращаем
/// `CmdOutcome::AddConflict` — UI откроет rename-модалку и сам решит,
/// под каким именем отправить повтор.
///
/// `filename`:
/// * `None` — первая попытка; имя выведет демон из Content-Disposition
///   или хвоста URL.
/// * `Some(name)` — явно заданное пользователем имя (уже после rename-модалки).
pub fn add(
    ch: AuthedChannel,
    tx: UnboundedSender<UiEvent>,
    form: AddForm,
    filename: Option<String>,
) {
    tokio::spawn(async move {
        let mut client = BrookServiceClient::new(ch);
        let spec = FileSpec {
            url: form.url.clone(),
            target_dir: form.folder.clone(),
            filename: filename.clone(),
        };
        match client.add(AddRequest { spec: Some(spec) }).await {
            Ok(_) => {
                send(
                    &tx,
                    CmdOutcome::AddAccepted {
                        renamed_to: filename,
                    },
                );
            }
            Err(st) if st.code() == Code::AlreadyExists => {
                // Демон кладёт имя в сообщение строго как
                // `"file already exists: <name>"` (см. brook-core Error::FileExists).
                // Если парсинг не удался — fallback на хвост URL; если и это
                // не даёт имени, отдаём общую ошибку: без базового имени
                // подсказать в модалке нечего.
                let base = parse_existing_filename(st.message())
                    .or_else(|| filename.clone())
                    .or_else(|| filename_from_url(&form.url));
                match base {
                    Some(base_name) => send(
                        &tx,
                        CmdOutcome::AddConflict {
                            base_name,
                            form: form.clone(),
                        },
                    ),
                    None => send(
                        &tx,
                        CmdOutcome::Error(
                            "file already exists; cannot derive filename from URL to rename".into(),
                        ),
                    ),
                }
            }
            Err(st) => {
                send(
                    &tx,
                    CmdOutcome::Error(format!("add failed: {}", st.message())),
                );
            }
        }
    });
}

/// Вставить счётчик перед последней точкой расширения — Windows
/// Explorer / macOS Finder convention. `file.bin` → `file (1).bin`;
/// `README` → `README (1)`; `.bashrc` → `.bashrc (1)` (ведущая точка
/// трактуется как часть stem, расширения нет); `a.tar.gz` → `a.tar (1).gz`.
pub(crate) fn apply_counter(original: &str, n: u32) -> String {
    match original.rfind('.') {
        Some(pos) if pos > 0 => {
            let (stem, dot_ext) = original.split_at(pos);
            let ext = &dot_ext[1..];
            format!("{stem} ({n}).{ext}")
        }
        _ => format!("{original} ({n})"),
    }
}

/// Вытащить последний сегмент пути URL: `https://h/a/b/c.bin?x=1#y` → `c.bin`.
pub(crate) fn filename_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = after_scheme.split_once('/').map(|(_, rest)| rest)?;
    let path = path.split(['?', '#']).next().unwrap_or("");
    let last = path.rsplit('/').find(|s| !s.is_empty())?;
    Some(last.to_owned())
}

/// Парсер сообщения ошибки демона. Формат строго
/// `"file already exists: <name>"` — см. `brook_core::Error::FileExists`
/// в [crates/brook-core/src/error.rs]. Если prefix не совпал, возвращаем
/// `None` — вызывающая сторона сама решит, откуда взять base-имя.
fn parse_existing_filename(msg: &str) -> Option<String> {
    const PREFIX: &str = "file already exists: ";
    msg.strip_prefix(PREFIX)
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn id_request(id: &str) -> IdRequest {
    IdRequest {
        id: Some(FileId { value: id.into() }),
    }
}

pub fn pause(ch: AuthedChannel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    run_bulk(ch, tx, ids, "pause", |mut client, req| {
        Box::pin(async move { client.pause(req).await.map(|_| ()) })
    });
}

pub fn resume(ch: AuthedChannel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    run_bulk(ch, tx, ids, "resume", |mut client, req| {
        Box::pin(async move { client.resume(req).await.map(|_| ()) })
    });
}

pub fn retry(ch: AuthedChannel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    run_bulk(ch, tx, ids, "retry", |mut client, req| {
        Box::pin(async move { client.retry(req).await.map(|_| ()) })
    });
}

/// macOS Finder reveal: `open -R <path>`. Ошибки игнорируем —
/// «открыть» не часть контракта UI, а удобство.
pub fn reveal_in_finder(target_dir: &str, filename: &str) {
    if target_dir.is_empty() || filename.is_empty() {
        return;
    }
    let path = std::path::Path::new(target_dir).join(filename);
    let _ = std::process::Command::new("open")
        .arg("-R")
        .arg(path)
        .spawn();
}

/// Remove идемпотентен на стороне демона (см. `manager::remove`): ghost
/// id даст `Ok`. Успешно обработанные id отдаём UI, чтобы тот дропнул
/// их из ViewModel — событий по удалению демон не генерит.
pub fn remove(ch: AuthedChannel, tx: UnboundedSender<UiEvent>, ids: Vec<String>) {
    tokio::spawn(async move {
        let mut removed: Vec<String> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        for id in &ids {
            let mut client = BrookServiceClient::new(ch.clone());
            let req = RemoveRequest {
                id: Some(FileId { value: id.clone() }),
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
    BrookServiceClient<AuthedChannel>,
    IdRequest,
)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Status>> + Send>>;

fn run_bulk(
    ch: AuthedChannel,
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
pub fn shutdown_daemon(ch: AuthedChannel, tx: UnboundedSender<UiEvent>) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_counter_handles_extension() {
        assert_eq!(apply_counter("f.bin", 1), "f (1).bin");
        assert_eq!(apply_counter("f.bin", 42), "f (42).bin");
    }

    #[test]
    fn apply_counter_uses_last_extension() {
        // `.tar.gz` лечится как один «последний» суффикс — это
        // компромисс ради простоты кода. Даёт «a.tar (1).gz».
        assert_eq!(apply_counter("a.tar.gz", 1), "a.tar (1).gz");
    }

    #[test]
    fn apply_counter_no_extension() {
        assert_eq!(apply_counter("README", 2), "README (2)");
    }

    #[test]
    fn apply_counter_dotfile_treated_as_no_extension() {
        assert_eq!(apply_counter(".bashrc", 1), ".bashrc (1)");
    }

    #[test]
    fn filename_from_url_strips_query_and_fragment() {
        assert_eq!(
            filename_from_url("https://a/b/c/d.bin?x=1#y").as_deref(),
            Some("d.bin")
        );
        assert_eq!(
            filename_from_url("https://a/b/c/d.bin").as_deref(),
            Some("d.bin")
        );
        assert_eq!(filename_from_url("https://a/").as_deref(), None);
        assert_eq!(filename_from_url("https://a").as_deref(), None);
    }

    #[test]
    fn parse_existing_filename_matches_daemon_format() {
        assert_eq!(
            parse_existing_filename("file already exists: large").as_deref(),
            Some("large")
        );
        assert_eq!(
            parse_existing_filename("file already exists: movie (1).mp4").as_deref(),
            Some("movie (1).mp4")
        );
        assert_eq!(
            parse_existing_filename("file already exists: ").as_deref(),
            None
        );
        assert_eq!(parse_existing_filename("something else").as_deref(), None);
    }
}
