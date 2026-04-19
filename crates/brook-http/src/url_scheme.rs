//! Валидация схемы URL на границе адаптера.
//!
//! Адаптер принимает только `http` и `https`. Всё остальное отсекается **до**
//! любого сетевого обращения — это контракт портов `brook-core`.

use url::Url;

/// Проверяет, что строка — валидный URL со схемой `http`/`https`.
///
/// На успех возвращает распарсенный `Url`; иначе — строку-ошибку для маппинга
/// в доменные `InvalidScheme` / `Malformed`.
pub(crate) fn validate_http_url(raw: &str) -> Result<Url, String> {
    let parsed = Url::parse(raw).map_err(|e| format!("invalid URL `{raw}`: {e}"))?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed),
        other => Err(other.to_string()),
    }
}
