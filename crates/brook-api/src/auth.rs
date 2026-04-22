//! Bearer-аутентификация для gRPC.
//!
//! На сервере: интерцептор сверяет `authorization: bearer <pass>` с
//! ожидаемым значением константным временем (`subtle`). Отсутствие /
//! несовпадение — `Status::unauthenticated`. Если демон поднят без
//! пароля (локальный loopback), интерцептор пропускает всё как есть.
//!
//! На клиенте: симметричный интерцептор, который подсовывает тот же
//! заголовок в исходящие запросы. Живёт в `brook-tui`, см. `connect.rs`.
//!
//! Имя заголовка и префикс берём из [`brook_runtime::constants`] — чтобы
//! сервер и клиент не разъехались из-за хардкодов.

use std::sync::Arc;

use brook_runtime::constants::{
    AUTH_HEADER,
    AUTH_SCHEME,
};
use subtle::ConstantTimeEq;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;

use crate::trace::trace_interceptor;

/// Интерцептор-«комбайн»: сначала проверяет bearer (если задан
/// `expected`), затем — стандартный trace. Порядок важен: без
/// аутентификации незачем генерить correlation-id и писать лог.
#[derive(Clone, Default)]
pub struct AuthInterceptor {
    /// `None` — пароль не настроен, пропускаем всё подряд (dev-режим на
    /// loopback). `Some` — каждый запрос обязан предъявить bearer.
    expected: Option<Arc<String>>,
}

impl AuthInterceptor {
    pub fn new(expected: Option<Arc<String>>) -> Self {
        Self { expected }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(expected) = self.expected.as_deref() {
            check_bearer(&req, expected)?;
        }
        trace_interceptor(req)
    }
}

/// Проверить, что `authorization: bearer <token>` присутствует и
/// совпадает с `expected`. Сравнение — константного времени (`subtle`),
/// чтобы по латентности нельзя было скармливать префикс.
fn check_bearer(req: &tonic::Request<()>, expected: &str) -> Result<(), Status> {
    let raw: &MetadataValue<_> = req
        .metadata()
        .get(AUTH_HEADER)
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
    let raw = raw
        .to_str()
        .map_err(|_| Status::unauthenticated("authorization header is not ASCII"))?;
    // Schema — регистронезависимая (RFC 6750). `strip_prefix` не умеет
    // case-insensitive, поэтому сравниваем длину + lowercase префикс.
    let prefix_len = AUTH_SCHEME.len();
    if raw.len() < prefix_len || !raw[..prefix_len].eq_ignore_ascii_case(AUTH_SCHEME) {
        return Err(Status::unauthenticated("expected `bearer <token>` scheme"));
    }
    let token = &raw[prefix_len..];
    let eq: bool = token.as_bytes().ct_eq(expected.as_bytes()).into();
    if !eq {
        return Err(Status::unauthenticated("invalid token"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with(header: Option<&str>) -> tonic::Request<()> {
        let mut r = tonic::Request::new(());
        if let Some(v) = header {
            r.metadata_mut().insert(AUTH_HEADER, v.parse().unwrap());
        }
        r
    }

    #[test]
    fn missing_header_is_unauthenticated() {
        let r = req_with(None);
        let err = check_bearer(&r, "s3cr3t").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn wrong_scheme_is_unauthenticated() {
        let r = req_with(Some("Basic s3cr3t"));
        let err = check_bearer(&r, "s3cr3t").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn wrong_token_is_unauthenticated() {
        let r = req_with(Some("bearer wrong"));
        let err = check_bearer(&r, "s3cr3t").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn correct_token_passes() {
        let r = req_with(Some("bearer s3cr3t"));
        check_bearer(&r, "s3cr3t").unwrap();
    }

    #[test]
    fn case_insensitive_scheme() {
        let r = req_with(Some("Bearer s3cr3t"));
        check_bearer(&r, "s3cr3t").unwrap();
    }

    #[test]
    fn interceptor_without_expected_is_passthrough() {
        let mut it = AuthInterceptor::new(None);
        it.call(tonic::Request::new(())).unwrap();
    }

    #[test]
    fn interceptor_with_expected_rejects_missing_header() {
        let mut it = AuthInterceptor::new(Some(Arc::new("s3cr3t".into())));
        let err = it.call(tonic::Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
