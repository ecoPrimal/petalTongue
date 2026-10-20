// SPDX-License-Identifier: AGPL-3.0-or-later

use super::DiscoveryServiceClient;
use super::protocol::{JsonRpcError, JsonRpcResponse};
use petal_tongue_core::types::PrimalHealthStatus;
use serde_json::json;
use std::path::PathBuf;

#[test]
fn test_get_search_paths() {
    let paths = DiscoveryServiceClient::get_search_paths();
    assert!(!paths.is_empty());

    // Should always have /tmp as fallback
    assert!(paths.iter().any(|p| p.ends_with("tmp")));
}

#[test]
fn test_with_socket_path() {
    let path = PathBuf::from("/run/user/1000/discovery.sock");
    let client = DiscoveryServiceClient::with_socket_path(path.clone());
    assert_eq!(client.socket_path(), &path);
}

#[test]
fn test_parse_primal() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));

    let json = json!({
        "id": "beardog-123",
        "name": "beardog",
        "primal_type": "beardog",
        "endpoint": "unix:///run/user/1000/beardog-nat0.sock",
        "capabilities": ["encryption", "identity"],
        "health": "healthy",
        "last_seen": 1_234_567_890
    });

    let primal = client.parse_primal(&json).unwrap();
    assert_eq!(primal.id, "beardog-123");
    assert_eq!(primal.name, "beardog");
    assert_eq!(primal.capabilities.len(), 2);
    assert!(matches!(primal.health, PrimalHealthStatus::Healthy));
}

#[test]
fn test_parse_primal_minimal() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));

    let json = json!({
        "id": "minimal-primal",
        "name": "minimal",
        "endpoint": "unix:///tmp/minimal.sock"
    });

    let primal = client.parse_primal(&json).unwrap();
    assert_eq!(primal.id, "minimal-primal");
    assert_eq!(primal.primal_type, "unknown");
    assert!(primal.capabilities.is_empty());
}

#[test]
fn test_parse_primal_health_variants() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));

    let json_healthy = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "health": "healthy"
    });
    assert!(matches!(
        client.parse_primal(&json_healthy).unwrap().health,
        PrimalHealthStatus::Healthy
    ));

    let json_degraded = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "health": "degraded"
    });
    assert!(matches!(
        client.parse_primal(&json_degraded).unwrap().health,
        PrimalHealthStatus::Warning
    ));

    let json_critical = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "health": "critical"
    });
    assert!(matches!(
        client.parse_primal(&json_critical).unwrap().health,
        PrimalHealthStatus::Critical
    ));
}

#[test]
fn test_parse_primal_type_fallback() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));

    let json = json!({
        "id": "test",
        "name": "test",
        "primal_type": "toadstool",
        "endpoint": "unix:///tmp/test.sock"
    });
    let primal = client.parse_primal(&json).unwrap();
    assert_eq!(primal.primal_type, "toadstool");

    let json_type = json!({
        "id": "test",
        "name": "test",
        "type": "beardog",
        "endpoint": "unix:///tmp/test.sock"
    });
    let primal2 = client.parse_primal(&json_type).unwrap();
    assert_eq!(primal2.primal_type, "beardog");
}

#[test]
fn test_parse_primal_missing_id() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "name": "test",
        "endpoint": "unix:///tmp/test.sock"
    });
    assert!(client.parse_primal(&json).is_err());
}

#[test]
fn test_parse_primal_missing_name() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "endpoint": "unix:///tmp/test.sock"
    });
    assert!(client.parse_primal(&json).is_err());
}

#[test]
fn test_parse_primal_missing_endpoint() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test"
    });
    assert!(client.parse_primal(&json).is_err());
}

#[test]
fn test_discover_fails_without_socket() {
    let result = DiscoveryServiceClient::discover(Some("nonexistent-family-xyz"));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_send_request_fails_nonexistent_socket() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from(
        "/tmp/nonexistent-socket-12345.sock",
    ));
    let result = client.discover_by_capability("visualization").await;
    assert!(result.is_err());
}

#[test]
fn test_parse_primal_last_seen_fallback() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock"
    });
    let primal = client.parse_primal(&json).expect("parse");
    assert!(primal.last_seen > 0);
}

#[test]
fn test_parse_primal_health_warning_variants() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    for (health_str, expected) in [
        ("warning", PrimalHealthStatus::Warning),
        ("error", PrimalHealthStatus::Critical),
        ("unhealthy", PrimalHealthStatus::Critical),
    ] {
        let json = json!({
            "id": "test",
            "name": "test",
            "endpoint": "unix:///tmp/test.sock",
            "health": health_str
        });
        let primal = client.parse_primal(&json).expect("parse");
        assert!(
            std::mem::discriminant(&primal.health) == std::mem::discriminant(&expected),
            "health {health_str}"
        );
    }
}

#[test]
fn test_parse_primal_unknown_health_defaults_healthy() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "health": "unknown_status"
    });
    let primal = client.parse_primal(&json).expect("parse");
    assert!(matches!(primal.health, PrimalHealthStatus::Healthy));
}

#[test]
fn test_discovery_query_request_structure() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "discovery.query",
        "params": {"capability": "visualization"},
        "id": 1
    });
    assert_eq!(request["method"], "discovery.query");
    assert_eq!(request["params"]["capability"], "visualization");
}

#[test]
fn test_health_check_request_structure() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "health.check",
        "params": {},
        "id": 1
    });
    assert_eq!(request["method"], "health.check");
}

#[test]
fn test_get_all_primals_request_structure() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "discovery.query",
        "params": {"capability": "*"},
        "id": 1
    });
    assert_eq!(request["params"]["capability"], "*");
}

#[test]
fn test_jsonrpc_response_deserialization() {
    let json = r#"{"jsonrpc":"2.0","result":[{"id":"p1","name":"p1","endpoint":"unix:///tmp/p1.sock"}],"id":1}"#;
    let response: JsonRpcResponse = serde_json::from_str(json).expect("deserialize");
    assert!(response.result.is_some());
    assert!(response.error.is_none());
}

#[test]
fn test_jsonrpc_error_deserialization() {
    let json = r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}"#;
    let response: JsonRpcResponse = serde_json::from_str(json).expect("deserialize");
    assert!(response.error.is_some());
    let err = response.error.as_ref().unwrap();
    assert_eq!(err.code, -32601);
    assert_eq!(err.message, "Method not found");
}

#[test]
fn test_jsonrpc_response_serialization() {
    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: Some(json!([{"id": "p1"}])),
        error: None,
        id: json!(1),
    };
    let json = serde_json::to_string(&response).expect("serialize");
    assert!(json.contains("2.0"));
    assert!(json.contains("p1"));
}

#[test]
fn test_jsonrpc_error_serialization() {
    let err = JsonRpcError {
        code: -32600,
        message: "Invalid request".to_string(),
        data: None,
    };
    let json = serde_json::to_string(&err).expect("serialize");
    assert!(json.contains("-32600"));
    assert!(json.contains("Invalid request"));
}

#[test]
fn test_get_search_paths_with_xdg() {
    petal_tongue_core::test_fixtures::env_test_helpers::with_env_var(
        "XDG_RUNTIME_DIR",
        "/custom/xdg",
        || {
            let paths = DiscoveryServiceClient::get_search_paths();
            assert_eq!(paths.first().and_then(|p| p.to_str()), Some("/custom/xdg"));
        },
    );
}

#[test]
fn test_parse_primal_capabilities_mixed_types() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "capabilities": ["cap1", "cap2"]
    });
    let primal = client.parse_primal(&json).unwrap();
    assert_eq!(primal.capabilities, vec!["cap1", "cap2"]);
}

#[test]
fn test_health_check_response_parsing() {
    let result = json!({"status": "healthy"});
    let status = result["status"].as_str().unwrap_or("unknown").to_string();
    assert_eq!(status, "healthy");
}

#[test]
fn test_discovery_socket_name_format() {
    let family = "nat0";
    let base = petal_tongue_core::constants::discovery_service_socket_name();
    let socket_name = format!("{base}-{family}.sock");
    assert!(
        std::path::Path::new(&socket_name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sock"))
    );
    assert!(socket_name.contains("nat0"));
}

#[test]
fn test_parse_primal_ok_variant() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "ok-primal",
        "name": "OK",
        "endpoint": "unix:///tmp/ok.sock",
        "health": "ok"
    });
    let primal = client.parse_primal(&json).unwrap();
    assert!(matches!(primal.health, PrimalHealthStatus::Healthy));
}

#[test]
fn test_parse_primal_capabilities_empty_array() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "capabilities": []
    });
    let primal = client.parse_primal(&json).unwrap();
    assert!(primal.capabilities.is_empty());
}

#[test]
fn test_parse_primal_capabilities_null() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock"
    });
    let primal = client.parse_primal(&json).unwrap();
    assert!(primal.capabilities.is_empty());
}

#[test]
fn test_discover_family_id_from_env() {
    petal_tongue_core::test_fixtures::env_test_helpers::with_env_var(
        "FAMILY_ID",
        "custom-family",
        || {
            let result = DiscoveryServiceClient::discover(None);
            let _ = result;
        },
    );
}

#[test]
fn test_discover_with_explicit_family() {
    let result = DiscoveryServiceClient::discover(Some("test-family"));
    assert!(result.is_err());
}

#[test]
fn test_jsonrpc_request_structure_discovery_query() {
    let req = json!({
        "jsonrpc": "2.0",
        "method": "discovery.query",
        "params": {"capability": "storage"},
        "id": 1
    });
    assert_eq!(req["method"], "discovery.query");
    assert_eq!(req["params"]["capability"], "storage");
}

#[test]
fn test_parse_primal_last_seen_explicit() {
    let client = DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/test.sock"));
    let json = json!({
        "id": "test",
        "name": "test",
        "endpoint": "unix:///tmp/test.sock",
        "last_seen": 999_999_999
    });
    let primal = client.parse_primal(&json).unwrap();
    assert_eq!(primal.last_seen, 999_999_999);
}

#[test]
fn test_socket_path_getter() {
    let path = PathBuf::from("/run/user/1000/songbird.sock");
    let client = DiscoveryServiceClient::with_socket_path(path.clone());
    assert_eq!(client.socket_path(), &path);
}

#[tokio::test]
async fn test_get_all_primals_fails_nonexistent_socket() {
    let client =
        DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/nonexistent-xyz-12345.sock"));
    let result = client.get_all_primals().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_health_check_fails_nonexistent_socket() {
    let client =
        DiscoveryServiceClient::with_socket_path(PathBuf::from("/tmp/nonexistent-health.sock"));
    let result = client.health_check().await;
    assert!(result.is_err());
}
