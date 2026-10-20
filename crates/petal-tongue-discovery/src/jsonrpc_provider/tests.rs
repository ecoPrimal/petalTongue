// SPDX-License-Identifier: AGPL-3.0-or-later
//! JSON-RPC provider tests

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::traits::VisualizationDataProvider;

use super::JsonRpcProvider;
use super::types::{JsonRpcRequest, JsonRpcResponse};

async fn create_mock_server(socket_path: &str) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)?;

    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(handle_mock_connection(stream));
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok(handle)
}

async fn handle_mock_connection(stream: UnixStream) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await.is_ok() && !line.is_empty() {
        if let Ok(request) = serde_json::from_str::<JsonRpcRequest>(&line) {
            let req_id = request.id.unwrap_or(serde_json::Value::Null);
            let response = match request.method.as_str() {
                "primal.list" => JsonRpcResponse::success(
                    req_id,
                    serde_json::json!([
                        {
                            "id": "test-primal",
                            "name": "Test Primal",
                            "primal_type": "test",
                            "endpoint": "unix:///tmp/test.sock",
                            "capabilities": ["test"],
                            "health": "Healthy",
                            "last_seen": 0
                        }
                    ]),
                ),
                "topology.get" | "get_topology" => {
                    JsonRpcResponse::success(req_id, serde_json::json!([]))
                }
                _ => JsonRpcResponse::error(req_id, -32601, "Method not found"),
            };

            let response_json = serde_json::to_string(&response).unwrap() + "\n";
            let _ = writer.write_all(response_json.as_bytes()).await;
            let _ = writer.flush().await;
        }

        line.clear();
    }
}

#[tokio::test]
async fn test_jsonrpc_request_serialization() {
    let request = JsonRpcRequest::new("primal.list", None, 1);

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("\"jsonrpc\":\"2.0\""));
    assert!(json.contains("\"method\":\"primal.list\""));
    assert!(json.contains("\"id\":1"));
}

#[tokio::test]
async fn test_jsonrpc_response_deserialization() {
    let json = r#"{"jsonrpc":"2.0","result":[],"id":1}"#;
    let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
    assert_eq!(response.jsonrpc, "2.0");
    assert_eq!(response.id, serde_json::json!(1));
    assert!(response.error.is_none());
}

#[tokio::test]
async fn test_jsonrpc_error_response() {
    let json = r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}"#;
    let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
    assert!(response.error.is_some());
    assert_eq!(response.error.unwrap().code, -32601);
}

#[tokio::test]
async fn test_jsonrpc_provider_get_primals() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("primals.sock");
    let path_str = socket_path.to_str().unwrap();
    let _server = create_mock_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(path_str);
    let primals = provider.get_primals().await.unwrap();

    assert_eq!(primals.len(), 1);
    assert_eq!(primals[0].id, "test-primal");
}

#[tokio::test]
async fn test_jsonrpc_provider_health_check() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("health.sock");
    let path_str = socket_path.to_str().unwrap();
    let _server = create_mock_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(path_str);
    let health = provider.health_check().await.unwrap();

    assert!(health.contains("healthy"));
}

#[tokio::test]
async fn test_jsonrpc_provider_get_topology() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("topology.sock");
    let path_str = socket_path.to_str().unwrap();
    let _server = create_mock_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(path_str);
    let topology = provider.get_topology().await.unwrap();

    assert_eq!(topology.len(), 0);
}

#[tokio::test]
async fn test_jsonrpc_provider_metadata() {
    let provider = JsonRpcProvider::new("/tmp/test.sock");
    let metadata = provider.get_metadata();

    assert_eq!(metadata.name, "JSON-RPC Provider");
    assert_eq!(metadata.protocol, "jsonrpc-2.0");
    assert!(metadata.capabilities.contains(&"primals".to_string()));
}

#[tokio::test]
async fn test_jsonrpc_request_id_increment() {
    let provider = JsonRpcProvider::new("/tmp/test.sock");
    let id1 = provider.request_id.load(Ordering::SeqCst);
    provider.request_id.fetch_add(1, Ordering::SeqCst);
    let id2 = provider.request_id.load(Ordering::SeqCst);

    assert_eq!(id2, id1 + 1);
}

#[tokio::test]
async fn test_standard_socket_paths() {
    let paths = JsonRpcProvider::get_standard_socket_paths().unwrap();

    assert!(paths.len() >= 4);
    assert!(
        paths
            .iter()
            .any(|p| p.to_string_lossy().contains("biomeos"))
    );
}

#[tokio::test]
#[cfg(feature = "test-fixtures")]
async fn test_jsonrpc_discover_with_biomeos_url() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("discover.sock");
    let path_str = socket_path.to_str().unwrap();
    let _server = create_mock_server(path_str).await.unwrap();

    let result = petal_tongue_core::test_fixtures::env_test_helpers::with_env_var_async(
        "BIOMEOS_URL",
        &format!("unix://{path_str}"),
        || async { JsonRpcProvider::discover().await },
    )
    .await;

    assert!(result.is_ok());
    let provider = result.unwrap();
    assert!(provider.get_metadata().endpoint.contains("discover"));
}

#[tokio::test]
async fn test_jsonrpc_get_topology_method_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("topology-nf.sock");
    let path_str = socket_path.to_str().unwrap();
    let _server = create_mock_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(path_str);
    let topology = provider.get_topology().await.unwrap();

    assert_eq!(topology.len(), 0);
}

#[tokio::test]
async fn test_jsonrpc_provider_construction() {
    let provider = JsonRpcProvider::new("/nonexistent/socket.sock");
    let metadata = provider.get_metadata();
    assert_eq!(metadata.name, "JSON-RPC Provider");
    assert!(metadata.endpoint.contains("nonexistent"));
}

/// Mock server that returns malformed JSON
async fn create_malformed_server(socket_path: &str) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    let handle = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let (reader, mut writer) = stream.into_split();
            let mut reader = tokio::io::BufReader::new(reader);
            let mut line = String::new();
            // Wait for client request before responding
            let _ = reader.read_line(&mut line).await;
            let _ = writer.write_all(b"{ invalid json }\n").await;
            let _ = writer.flush().await;
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(handle)
}

/// Mock server that returns JSON-RPC error for primal.list
async fn create_jsonrpc_error_server(
    socket_path: &str,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (reader, mut writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                let mut line = String::new();
                if reader.read_line(&mut line).await.is_ok()
                    && let Ok(request) = serde_json::from_str::<JsonRpcRequest>(&line)
                {
                    let mut response = JsonRpcResponse::error(
                        request.id.unwrap_or(serde_json::Value::Null),
                        -32600,
                        "Invalid request",
                    );
                    if let Some(ref mut err) = response.error {
                        err.data = Some(serde_json::json!("extra"));
                    }
                    let response_json = serde_json::to_string(&response).unwrap() + "\n";
                    let _ = writer.write_all(response_json.as_bytes()).await;
                    let _ = writer.flush().await;
                }
            });
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(handle)
}

/// Mock server that returns wrong request ID
async fn create_wrong_id_server(socket_path: &str) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (reader, mut writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                let mut line = String::new();
                if reader.read_line(&mut line).await.is_ok() {
                    let response = JsonRpcResponse::success(
                        serde_json::json!(9999), // Wrong ID
                        serde_json::json!([]),
                    );
                    let response_json = serde_json::to_string(&response).unwrap() + "\n";
                    let _ = writer.write_all(response_json.as_bytes()).await;
                    let _ = writer.flush().await;
                }
            });
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(handle)
}

/// Mock server: topology.get returns -32601, `get_topology` returns empty array
async fn create_topology_fallback_server(
    socket_path: &str,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(handle_topology_fallback_connection(stream));
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(handle)
}

async fn handle_topology_fallback_connection(stream: UnixStream) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await.is_ok() && !line.is_empty() {
        if let Ok(request) = serde_json::from_str::<JsonRpcRequest>(&line) {
            let req_id = request.id.unwrap_or(serde_json::Value::Null);
            let response = match request.method.as_str() {
                "primal.list" => JsonRpcResponse::success(req_id, serde_json::json!([])),
                "get_topology" => JsonRpcResponse::success(
                    req_id,
                    serde_json::json!([
                        {"from": "a", "to": "b", "edge_type": "test"}
                    ]),
                ),
                "topology.get" | _ => {
                    JsonRpcResponse::error(req_id, -32601, "Method not found")
                }
            };
            let response_json = serde_json::to_string(&response).unwrap() + "\n";
            let _ = writer.write_all(response_json.as_bytes()).await;
            let _ = writer.flush().await;
        }
        line.clear();
    }
}

#[tokio::test]
async fn test_jsonrpc_get_primals_malformed_response() {
    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("malformed.sock");
    let path_str = sock_path.to_str().unwrap();
    let _server = create_malformed_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(sock_path);
    let result = provider.get_primals().await;

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    // Server sends malformed JSON; client may get ParseError or I/O (if connection closes early)
    assert!(
        err_str.contains("parse")
            || err_str.contains("Parse")
            || err_str.contains("JSON")
            || err_str.contains("I/O"),
        "expected parse or I/O error, got: {err_str}"
    );
}

#[tokio::test]
async fn test_jsonrpc_get_primals_jsonrpc_error_response() {
    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("error.sock");
    let path_str = sock_path.to_str().unwrap();
    let _server = create_jsonrpc_error_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(sock_path);
    let result = provider.get_primals().await;

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("JSON-RPC") || err_str.contains("Invalid request"),
        "expected JSON-RPC error, got: {err_str}"
    );
}

#[tokio::test]
async fn test_jsonrpc_get_primals_request_id_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("wrongid.sock");
    let path_str = sock_path.to_str().unwrap();
    let _server = create_wrong_id_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(sock_path);
    let result = provider.get_primals().await;

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("mismatch") || err_str.contains("Request ID"),
        "expected ID mismatch, got: {err_str}"
    );
}

#[tokio::test]
async fn test_jsonrpc_get_topology_fallback_to_get_topology() {
    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("topology-fallback.sock");
    let path_str = sock_path.to_str().unwrap();
    let _server = create_topology_fallback_server(path_str).await.unwrap();

    let provider = JsonRpcProvider::new(sock_path);
    let topology = provider.get_topology().await.unwrap();

    assert_eq!(topology.len(), 1);
    assert_eq!(topology[0].from.as_str(), "a");
    assert_eq!(topology[0].to.as_str(), "b");
}

#[tokio::test]
#[cfg(feature = "test-fixtures")]
async fn test_jsonrpc_discover_socket_not_found() {
    let result = petal_tongue_core::test_fixtures::env_test_helpers::with_env_var_async(
        "BIOMEOS_URL",
        "unix:///nonexistent/path/that/does/not/exist.sock",
        || async { JsonRpcProvider::discover().await },
    )
    .await;

    let err_str = match &result {
        Ok(_) => panic!("expected discover to fail"),
        Err(e) => e.to_string(),
    };
    assert!(
        err_str.contains("No JSON-RPC")
            || err_str.contains("not found")
            || err_str.contains("Socket"),
        "expected socket not found error, got: {err_str}"
    );
}

#[tokio::test]
async fn test_jsonrpc_connection_refused_nonexistent_socket() {
    let provider = JsonRpcProvider::new("/tmp/nonexistent-jsonrpc-xyz-98765.sock");
    let result = provider.health_check().await;
    assert!(result.is_err());
}
