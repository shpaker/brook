//! Опциональный `--directory`: проверяем семантику пары
//! `(directory, host)`.
//!
//! - `directory=None, host=loopback` → `OpenPathPolicy`, демон поднимается.
//! - `directory=None, host=non-loopback` → ошибка про `--directory` ещё
//!   до bind'а listener'а.

use std::net::IpAddr;
use std::path::Path;

use brook_daemon::ServerArgs;
use brook_daemon::app::{
    Paths,
    build_runtime,
    serve,
};
use brook_proto::brook::v1 as proto;
use brook_proto::brook::v1::brook_service_client::BrookServiceClient;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::transport::Channel;

fn write_minimal_config(dir: &Path) {
    // Без `download.default_dir` — проверяем, что демон сам резолвит prefill.
    let yaml = "\
download:
  piece_target_count: 1
  piece_size_min_mib: 1
  piece_size_max_mib: 1
api:
  port: 0
  bind: 127.0.0.1
log:
  dir: /tmp
  rotate_count: 1
  rotate_size_mb: 1
";
    std::fs::write(dir.join("brook.yaml"), yaml).unwrap();
}

async fn connect(addr: std::net::SocketAddr) -> Channel {
    let uri = format!("http://{addr}");
    for attempt in 0..40 {
        match Channel::from_shared(uri.clone()).unwrap().connect().await {
            Ok(c) => return c,
            Err(_) if attempt < 39 => {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await
            }
            Err(e) => panic!("connect failed: {e}"),
        }
    }
    unreachable!()
}

#[tokio::test(flavor = "multi_thread")]
async fn loopback_without_directory_starts_and_reports_default_dir() {
    let workdir = TempDir::new().unwrap();
    write_minimal_config(workdir.path());

    let paths = Paths::in_dir(workdir.path());
    let args = ServerArgs {
        directory: None,
        host: None,
        port: None,
        client_pass: None,
    };

    let runtime = build_runtime(&paths, &args).await.expect("build_runtime");
    let addr = runtime.addr;
    let (tx, rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        serve(runtime, async move {
            let _ = rx.await;
        })
        .await
    });

    let channel = connect(addr).await;
    let mut client = BrookServiceClient::new(channel);
    let resp = client
        .get_settings(proto::GetSettingsRequest {})
        .await
        .expect("GetSettings must succeed on loopback without --directory");
    assert!(
        !resp.into_inner().default_dir.is_empty(),
        "default_dir must be non-empty (UserDirs::download_dir or $HOME fallback)"
    );

    tx.send(()).unwrap();
    serve_task.await.expect("serve join").expect("serve result");
}

#[tokio::test(flavor = "multi_thread")]
async fn non_loopback_without_directory_refuses_to_start() {
    let workdir = TempDir::new().unwrap();
    write_minimal_config(workdir.path());

    let paths = Paths::in_dir(workdir.path());
    let args = ServerArgs {
        directory: None,
        host: Some(IpAddr::from([0, 0, 0, 0])),
        port: None,
        // Пароль задан, чтобы убедиться, что отказ — именно из-за directory,
        // а не из-за client_pass.
        client_pass: Some("p".into()),
    };

    match build_runtime(&paths, &args).await {
        Err(e) => {
            let msg = format!("{e:#}");
            assert!(
                msg.contains("--directory"),
                "expected --directory in error, got: {msg}"
            );
        }
        Ok(_) => panic!("must refuse non-loopback without --directory"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_arg_used_as_default_dir_when_yaml_silent() {
    let workdir = TempDir::new().unwrap();
    let sandbox = TempDir::new().unwrap();
    write_minimal_config(workdir.path());

    let paths = Paths::in_dir(workdir.path());
    let args = ServerArgs {
        directory: Some(sandbox.path().to_path_buf()),
        host: None,
        port: None,
        client_pass: None,
    };

    let runtime = build_runtime(&paths, &args).await.expect("build_runtime");
    let addr = runtime.addr;
    let (tx, rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        serve(runtime, async move {
            let _ = rx.await;
        })
        .await
    });

    let channel = connect(addr).await;
    let mut client = BrookServiceClient::new(channel);
    let resp = client
        .get_settings(proto::GetSettingsRequest {})
        .await
        .expect("GetSettings");
    let canonical_sandbox = std::fs::canonicalize(sandbox.path()).unwrap();
    let reported = std::path::PathBuf::from(resp.into_inner().default_dir);
    // `--directory` мог отдаться без канонизации, но всё равно должен
    // указывать на ту же директорию.
    let canonical_reported = std::fs::canonicalize(&reported).unwrap_or(reported.clone());
    assert_eq!(
        canonical_reported, canonical_sandbox,
        "default_dir should mirror --directory when YAML default_dir is unset"
    );

    tx.send(()).unwrap();
    serve_task.await.expect("serve join").expect("serve result");
}
