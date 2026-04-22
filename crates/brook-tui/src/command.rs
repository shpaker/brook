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

fn send(tx: &UnboundedSender<UiEvent>, outcome: CmdOutcome) {
    let _ = tx.send(UiEvent::CmdResult(outcome));
}

/// Сколько раз клиент переподберёт имя при повторном `AlreadyExists`.
/// Потолок защищает от зацикливания, если в каталоге уже много `(N)`.
const MAX_FILENAME_RETRIES: u32 = 100;

/// Послать `Add` и, если демон вернул `AlreadyExists`, автоматически
/// подобрать `<stem> (N).<ext>` и повторить вызов. UI-модалки для
/// конфликта имени больше нет — политика жёстко «rename на клиенте».
pub fn add(ch: Channel, tx: UnboundedSender<UiEvent>, form: AddForm) {
    tokio::spawn(async move {
        let mut client = BrookServiceClient::new(ch);
        // Базовое имя — хвост URL (после strip query/fragment). Если
        // вытащить не удалось, пусть решает демон: отправляем первый
        // запрос без `filename`, а при конфликте сдаёмся с тостом —
        // переподбирать без base-имени мы не можем.
        let base_name = filename_from_url(&form.url);

        // Первая попытка — без клиентского `filename` (пусть демон
        // выведёт из Content-Disposition/URL).
        let first = FileSpec {
            url: form.url.clone(),
            target_dir: form.folder.clone(),
            ..Default::default()
        };
        match client.add(AddRequest { spec: Some(first) }).await {
            Ok(_) => {
                send(&tx, CmdOutcome::AddAccepted { renamed_to: None });
                return;
            }
            Err(st) if st.code() == Code::AlreadyExists => {
                // Падаем в retry-loop ниже.
            }
            Err(st) => {
                send(
                    &tx,
                    CmdOutcome::Error(format!("add failed: {}", st.message())),
                );
                return;
            }
        }

        let Some(base) = base_name else {
            send(
                &tx,
                CmdOutcome::Error(
                    "file already exists; cannot derive filename from URL to rename".into(),
                ),
            );
            return;
        };

        for n in 1..=MAX_FILENAME_RETRIES {
            let candidate = apply_counter(&base, n);
            let spec = FileSpec {
                url: form.url.clone(),
                target_dir: form.folder.clone(),
                filename: Some(candidate.clone()),
            };
            match client.add(AddRequest { spec: Some(spec) }).await {
                Ok(_) => {
                    send(
                        &tx,
                        CmdOutcome::AddAccepted {
                            renamed_to: Some(candidate),
                        },
                    );
                    return;
                }
                Err(st) if st.code() == Code::AlreadyExists => {
                    // Продолжаем перебор.
                }
                Err(st) => {
                    send(
                        &tx,
                        CmdOutcome::Error(format!("add failed: {}", st.message())),
                    );
                    return;
                }
            }
        }
        send(
            &tx,
            CmdOutcome::Error(format!(
                "add failed: no free filename for {base} after {MAX_FILENAME_RETRIES} attempts"
            )),
        );
    });
}

/// Сконструировать кандидат `<stem> (N).<ext>` для retry. Повторяет
/// серверную схему rename'а, которую до рефакторинга делал демон.
fn apply_counter(original: &str, n: u32) -> String {
    // Ищем последнюю точку, которая не в начале, — это «расширение».
    // `a.tar.gz` → stem="a.tar", ext="gz". Имена без точки и дот-файлы
    // (`.bashrc`) трактуем как «нет расширения».
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
fn filename_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = after_scheme.split_once('/').map(|(_, rest)| rest)?;
    let path = path.split(['?', '#']).next().unwrap_or("");
    let last = path.rsplit('/').find(|s| !s.is_empty())?;
    Some(last.to_owned())
}

fn id_request(id: &str) -> IdRequest {
    IdRequest {
        id: Some(FileId { value: id.into() }),
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
}
