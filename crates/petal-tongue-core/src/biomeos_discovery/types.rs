// SPDX-License-Identifier: AGPL-3.0-or-later
//! Wire formats and [`BiomeOSDiscoveryEvent`].
//!
//! JSON-RPC types are re-exported from `cellmembrane-types` (canonical source).

use serde::{Deserialize, Serialize};

use crate::capability_discovery::{Capability, PrimalEndpoint, PrimalEndpoints, PrimalHealth};

// Canonical JSON-RPC 2.0 types from cellmembrane-types
pub use cellmembrane_types::{JsonRpcRequest, JsonRpcResponse};
// JsonRpcError is accessed transitively via JsonRpcResponse.error field

/// biomeOS primal format (from Neural API)
#[derive(Deserialize)]
pub struct BiomeOsPrimal {
    pub id: String,
    pub capabilities: Vec<String>,
    pub tarpc_endpoint: Option<String>,
    pub jsonrpc_endpoint: Option<String>,
    pub health: String,
}

impl From<BiomeOsPrimal> for PrimalEndpoint {
    fn from(p: BiomeOsPrimal) -> Self {
        Self {
            id: p.id,
            capabilities: p
                .capabilities
                .into_iter()
                .map(|cap| {
                    let parts: Vec<&str> = cap.split('.').collect();
                    if parts.len() == 2 {
                        Capability::new(parts[0]).with_operation(parts[1])
                    } else {
                        Capability::new(cap)
                    }
                })
                .collect(),
            endpoints: PrimalEndpoints {
                tarpc: p.tarpc_endpoint,
                jsonrpc: p.jsonrpc_endpoint,
                https: None,
            },
            health: match p.health.as_str() {
                "healthy" => PrimalHealth::Healthy,
                "degraded" => PrimalHealth::Degraded,
                _ => PrimalHealth::Unavailable,
            },
        }
    }
}

/// Real-time topology/health event from biomeOS WebSocket
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BiomeOSDiscoveryEvent {
    /// Primal health status changed
    PrimalStatus {
        /// ID of the primal whose status changed
        primal_id: String,
        /// New health status string (e.g. "healthy", "degraded")
        health: String,
    },
    /// Topology changed (primal added/removed or edge changed)
    TopologyUpdate {
        /// List of primal IDs in the topology
        primals: Vec<String>,
        /// Edges as (`from_id`, `to_id`) pairs
        edges: Vec<(String, String)>,
    },
}
