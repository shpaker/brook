//! Клиентский коннект: gRPC-канал + bearer-интерцептор.
//!
//! Сервер может работать как без пароля (loopback), так и с `--client-pass`.
//! Чтобы остальной TUI не разделял два кода пути, мы везде используем
//! [`AuthedChannel`] — обёртку над `Channel`, в которую интерцептор
//! всегда вставлен. При `None`-пароле он проходит пустым, т.е. сервер
//! получает запрос без лишнего заголовка.

use std::sync::Arc;

use brook_runtime::constants::{
    AUTH_HEADER,
    AUTH_SCHEME,
};
use tonic::Status;
use tonic::metadata::{
    AsciiMetadataValue,
    MetadataValue,
};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// Клиентский интерцептор, проставляющий `authorization: bearer <pass>`
/// в каждый исходящий запрос. Если пароль `None` — заголовок не
/// добавляется: для локального демона без auth'а это корректно.
#[derive(Clone, Default)]
pub struct BearerInterceptor {
    header: Option<AsciiMetadataValue>,
}

impl BearerInterceptor {
    pub fn new(pass: Option<Arc<String>>) -> Self {
        let header = pass.map(|p| {
            // Пароль уже провалидирован выше по стеку (prompt отверг бы
            // невалидные байты). `.expect` здесь — потому что невалидный
            // ASCII сломал бы gRPC и всё равно требовал panic'а.
            let v = format!("{AUTH_SCHEME}{p}");
            MetadataValue::try_from(v).expect("bearer value must be ASCII")
        });
        Self { header }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(h) = &self.header {
            req.metadata_mut().insert(AUTH_HEADER, h.clone());
        }
        Ok(req)
    }
}

/// Тип коннекта, который TUI таскает за собой. Под ним — обычный
/// `Channel` плюс `BearerInterceptor` (возможно, «пустой»).
pub type AuthedChannel = InterceptedService<Channel, BearerInterceptor>;

/// Собрать `AuthedChannel` из базового канала и опционального пароля.
pub fn wrap(channel: Channel, pass: Option<Arc<String>>) -> AuthedChannel {
    InterceptedService::new(channel, BearerInterceptor::new(pass))
}
