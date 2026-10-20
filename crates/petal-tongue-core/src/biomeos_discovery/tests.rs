// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::capability_discovery::{
    CapabilityQuery, DiscoveryBackend, DiscoveryError, PrimalEndpoint, PrimalHealth,
};
use crate::test_fixtures::env_test_helpers;

use super::backend::BiomeOsBackend;
use super::types::{BiomeOSDiscoveryEvent, BiomeOsPrimal, JsonRpcResponse};

#[test]
fn test_biomeos_backend_new() {
    let backend = BiomeOsBackend::new("/tmp/custom.sock");
    // Just verify it constructs - we can't call query without a real socket
    drop(backend);
}

#[test]
fn test_biomeos_from_env_explicit_socket() {
    let temp = std::env::temp_dir().join("biomeos-test-socket");
    std::fs::create_dir_all(temp.parent().unwrap()).unwrap();
    std::fs::write(&temp, "").unwrap();

    env_test_helpers::with_env_var("BIOMEOS_NEURAL_API_SOCKET", temp.to_str().unwrap(), || {
        let backend = BiomeOsBackend::from_env().unwrap();
        drop(backend);
    });

    let _ = std::fs::remove_file(&temp);
}

#[test]
fn test_biomeos_from_env_socket_not_found() {
    env_test_helpers::with_env_var(
        "BIOMEOS_NEURAL_API_SOCKET",
        "/nonexistent/path/neural-api.sock",
        || {
            let result = BiomeOsBackend::from_env();
            assert!(result.is_err());
            if let Err(DiscoveryError::BackendUnavailable(_)) = result {
                // Expected
            } else {
                panic!("Expected BackendUnavailable error");
            }
        },
    );
}

#[test]
fn test_biomeos_primal_conversion_healthy() {
    let biomeos_primal = BiomeOsPrimal {
        id: "test-primal-1".to_string(),
        capabilities: vec!["crypto.encrypt".to_string(), "crypto.decrypt".to_string()],
        tarpc_endpoint: Some("tarpc://unix:/run/primal/test".to_string()),
        jsonrpc_endpoint: Some("/run/primal/test.sock".to_string()),
        health: "healthy".to_string(),
    };

    let endpoint: PrimalEndpoint = biomeos_primal.into();
    assert_eq!(endpoint.id, "test-primal-1");
    assert_eq!(endpoint.capabilities.len(), 2);
    assert_eq!(endpoint.health, PrimalHealth::Healthy);
}

#[test]
fn test_biomeos_primal_conversion_degraded() {
    let biomeos_primal = BiomeOsPrimal {
        id: "degraded-primal".to_string(),
        capabilities: vec!["storage.cache".to_string()],
        tarpc_endpoint: None,
        jsonrpc_endpoint: Some("/run/degraded.sock".to_string()),
        health: "degraded".to_string(),
    };

    let endpoint: PrimalEndpoint = biomeos_primal.into();
    assert_eq!(endpoint.health, PrimalHealth::Degraded);
}

#[test]
fn test_biomeos_primal_conversion_unavailable() {
    let biomeos_primal = BiomeOsPrimal {
        id: "unavail-primal".to_string(),
        capabilities: vec![],
        tarpc_endpoint: None,
        jsonrpc_endpoint: None,
        health: "unavailable".to_string(),
    };

    let endpoint: PrimalEndpoint = biomeos_primal.into();
    assert_eq!(endpoint.health, PrimalHealth::Unavailable);
}

#[test]
fn test_biomeos_primal_conversion_unknown_health() {
    let biomeos_primal = BiomeOsPrimal {
        id: "unknown-primal".to_string(),
        capabilities: vec!["ui.render".to_string()],
        tarpc_endpoint: None,
        jsonrpc_endpoint: None,
        health: "unknown-status".to_string(),
    };

    let endpoint: PrimalEndpoint = biomeos_primal.into();
    assert_eq!(endpoint.health, PrimalHealth::Unavailable);
}

#[test]
fn test_biomeos_discovery_event_primal_status() {
    let json = r#"{"type":"PrimalStatus","primal_id":"p1","health":"healthy"}"#;
    let event: BiomeOSDiscoveryEvent = serde_json::from_str(json).expect("parse PrimalStatus");
    match &event {
        BiomeOSDiscoveryEvent::PrimalStatus { primal_id, health } => {
            assert_eq!(primal_id, "p1");
            assert_eq!(health, "healthy");
        }
        BiomeOSDiscoveryEvent::TopologyUpdate { .. } => panic!("expected PrimalStatus"),
    }
}

#[test]
fn test_biomeos_discovery_event_topology_update() {
    let json = r#"{"type":"TopologyUpdate","primals":["a","b"],"edges":[["a","b"]]}"#;
    let event: BiomeOSDiscoveryEvent = serde_json::from_str(json).expect("parse TopologyUpdate");
    match &event {
        BiomeOSDiscoveryEvent::TopologyUpdate { primals, edges } => {
            assert_eq!(primals, &["a", "b"]);
            assert_eq!(edges, &[("a".to_string(), "b".to_string())]);
        }
        BiomeOSDiscoveryEvent::PrimalStatus { .. } => panic!("expected TopologyUpdate"),
    }
}

#[test]
fn test_biomeos_primal_conversion_single_part_capability() {
    let biomeos_primal = BiomeOsPrimal {
        id: "legacy-primal".to_string(),
        capabilities: vec!["legacy".to_string()],
        tarpc_endpoint: None,
        jsonrpc_endpoint: None,
        health: "healthy".to_string(),
    };

    let endpoint: PrimalEndpoint = biomeos_primal.into();
    assert_eq!(endpoint.capabilities.len(), 1);
}

#[tokio::test]
async fn test_biomeos_query_unavailable_socket() {
    let backend = BiomeOsBackend::new("/nonexistent/path/neural-api-12345.sock");
    let query = CapabilityQuery {
        domain: "test".to_string(),
        operation: Some("op".to_string()),
        version_req: None,
    };
    let result = backend.query(&query).await;
    assert!(result.is_err());
    if let Err(DiscoveryError::CommunicationError(msg)) = result {
        assert!(!msg.is_empty());
    }
}

#[tokio::test]
async fn test_biomeos_subscribe_returns_ok() {
    let backend = BiomeOsBackend::new("/tmp/nonexistent.sock");
    let query = CapabilityQuery {
        domain: "test".to_string(),
        operation: None,
        version_req: None,
    };
    let result = backend.subscribe(&query).await;
    assert!(result.is_ok());
}

#[test]
fn test_jsonrpc_request_serialization() {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "discovery.query_capability",
        "params": {"domain": "crypto", "operation": "encrypt", "version_req": null},
        "id": 1
    });
    assert_eq!(request["method"], "discovery.query_capability");
    assert_eq!(request["params"]["domain"], "crypto");
}

#[test]
fn test_jsonrpc_response_error_parsing() {
    let json = r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"capability not found"},"id":1}"#;
    let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
    assert!(response.error.is_some());
    assert!(
        response
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("not found")
    );
}

#[test]
fn test_jsonrpc_response_result_parsing() {
    let json =
        r#"{"jsonrpc":"2.0","result":[{"id":"p1","capabilities":[],"health":"healthy"}],"id":1}"#;
    let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
    assert!(response.result.is_some());
}

#[test]
fn test_biomeos_discovery_event_serialization_roundtrip() {
    let json = r#"{"type":"PrimalStatus","primal_id":"p1","health":"degraded"}"#;
    let event: BiomeOSDiscoveryEvent = serde_json::from_str(json).expect("parse");
    let serialized = serde_json::to_string(&event).expect("serialize");
    let restored: BiomeOSDiscoveryEvent = serde_json::from_str(&serialized).expect("parse");
    match (&event, &restored) {
        (
            BiomeOSDiscoveryEvent::PrimalStatus {
                primal_id: a,
                health: b,
            },
            BiomeOSDiscoveryEvent::PrimalStatus {
                primal_id: c,
                health: d,
            },
        ) => {
            assert_eq!(a, c);
            assert_eq!(b, d);
        }
        _ => panic!("expected PrimalStatus"),
    }
}

#[test]
fn test_biomeos_discovery_event_topology_serialization() {
    let json = r#"{"type":"TopologyUpdate","primals":["a","b","c"],"edges":[["a","b"],["b","c"]]}"#;
    let event: BiomeOSDiscoveryEvent = serde_json::from_str(json).expect("parse");
    match &event {
        BiomeOSDiscoveryEvent::TopologyUpdate { primals, edges } => {
            assert_eq!(primals.len(), 3);
            assert_eq!(edges.len(), 2);
        }
        BiomeOSDiscoveryEvent::PrimalStatus { .. } => panic!("expected TopologyUpdate"),
    }
}

#[test]
fn test_biomeos_primal_capability_domain_operation_parsing() {
    let biomeos_primal = BiomeOsPrimal {
        id: "cap-test".to_string(),
        capabilities: vec!["domain.operation".to_string(), "single".to_string()],
        tarpc_endpoint: None,
        jsonrpc_endpoint: None,
        health: "healthy".to_string(),
    };
    let endpoint: PrimalEndpoint = biomeos_primal.into();
    assert_eq!(endpoint.capabilities.len(), 2);
}
