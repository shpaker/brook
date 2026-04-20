//! gRPC-интерцептор для трейсинга.
//!
//! На каждый входящий запрос:
//! - достаём `session-id` и `request-id` из метадаты (или генерим новые
//!   UUIDv4, если клиент не прислал);
//! - кладём их в `tonic::Extensions` запроса — `BrookService` может
//!   читать в методах, если понадобится;
//! - открываем `tracing::info_span!("grpc.request", ...)` — дочерние
//!   core-операции автоматически наследуют корреляционные поля.
//!
//! В MVP это всё — никакого сбора метрик или логов тела. `brookd` в §4.2
//! подключает интерцептор одной строкой:
//! `Server::builder().add_service(BrookServiceServer::with_interceptor(svc, trace_interceptor))`.

use tonic::Status;
use tonic::service::Interceptor;
use tracing::Span;
use uuid::Uuid;

/// Корреляционные id, прокинутые в запрос через `Extensions`.
#[derive(Clone, Debug)]
pub struct CorrelationIds {
    pub session_id: String,
    pub request_id: String,
}

fn metadata_str(req: &tonic::Request<()>, key: &'static str) -> Option<String> {
    req.metadata()
        .get(key)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

/// Тонкий интерцептор — вызывается tonic'ом на каждый входящий запрос.
pub fn trace_interceptor(mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
    let session_id = metadata_str(&req, "session-id").unwrap_or_else(|| Uuid::new_v4().to_string());
    let request_id = metadata_str(&req, "request-id").unwrap_or_else(|| Uuid::new_v4().to_string());

    // Root-span на запрос. `Span::record` ограничен полями, объявленными
    // при создании span'а, поэтому имена регистрируем заранее.
    let span =
        tracing::info_span!("grpc.request", session_id = %session_id, request_id = %request_id);
    let _enter = span.enter();
    Span::current().record("session_id", tracing::field::display(&session_id));
    Span::current().record("request_id", tracing::field::display(&request_id));

    req.extensions_mut().insert(CorrelationIds {
        session_id,
        request_id,
    });
    Ok(req)
}

/// Удобная обёртка: тип-маркер, реализующий `Interceptor`. Позволяет
/// использовать `BrookServiceServer::with_interceptor(svc, TraceInterceptor)`
/// на стороне `brookd`.
#[derive(Clone, Copy, Default)]
pub struct TraceInterceptor;

impl Interceptor for TraceInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        trace_interceptor(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_injects_correlation_when_headers_missing() {
        let req = tonic::Request::new(());
        let out = trace_interceptor(req).unwrap();
        let ids: &CorrelationIds = out.extensions().get().expect("ids present");
        assert!(!ids.session_id.is_empty());
        assert!(!ids.request_id.is_empty());
        assert_ne!(ids.session_id, ids.request_id);
    }

    #[test]
    fn interceptor_preserves_client_headers() {
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("session-id", "sess-123".parse().unwrap());
        req.metadata_mut()
            .insert("request-id", "req-456".parse().unwrap());
        let out = trace_interceptor(req).unwrap();
        let ids: &CorrelationIds = out.extensions().get().unwrap();
        assert_eq!(ids.session_id, "sess-123");
        assert_eq!(ids.request_id, "req-456");
    }
}
