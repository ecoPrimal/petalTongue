// SPDX-License-Identifier: AGPL-3.0-or-later
//! JSON-RPC provider implementation

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use petal_tongue_core::transport::{TransportEndpoint, connect_transport};
use petal_tongue_core::{PrimalInfo, TopologyEdge};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, info, warn};

use petal_tongue_core::constants::{
    biomeos_device_management_socket_name, biomeos_legacy_socket_name, biomeos_ui_socket_name,
    discovery_service_socket_name,
};

use crate::errors::{DiscoveryError, DiscoveryResult};
use crate::traits::{ProviderMetadata, VisualizationDataProvider};

use super::types::{JsonRpcProvider, JsonRpcRequest, JsonRpcResponse};

impl JsonRpcProvider {
    /// Create a new JSON-RPC provider for the given Unix socket path
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            request_id: AtomicU64::new(1),
            timeout: Duration::from_secs(10),
        }
    }

    /// Auto-discover JSON-RPC providers on standard Unix socket paths
    ///
    /// # Errors
    /// Returns `DiscoveryError` if no socket found or connection test fails.
    pub async fn discover() -> DiscoveryResult<Self> {
        info!("🔍 Auto-discovering JSON-RPC providers on Unix sockets...");

        if let Ok(url) = std::env::var("BIOMEOS_URL")
            && let Some(socket_path) = url.strip_prefix("unix://")
        {
            debug!("Using BIOMEOS_URL: {}", socket_path);
            if tokio::fs::metadata(socket_path).await.is_ok() {
                info!("✅ Found JSON-RPC provider at {}", socket_path);
                return Ok(Self::new(socket_path));
            }
            warn!(
                "❌ Socket specified in BIOMEOS_URL not found: {}",
                socket_path
            );
        }

        let standard_paths = Self::get_standard_socket_paths()?;

        for path in standard_paths {
            debug!("Checking for socket at: {}", path.display());
            if tokio::fs::metadata(&path).await.is_ok()
                && Self::test_connection(&path).await.is_ok()
            {
                info!("✅ Discovered JSON-RPC provider at {}", path.display());
                return Ok(Self::new(path));
            }
        }

        Err(DiscoveryError::NoJsonRpcProvidersFound {
            message: format!(
                "Tried DH-1 search dirs ({} dirs) with socket names: {}, {}, {}, {}. \
                 Set BIOMEOS_URL=unix:///path/to/socket or BIOMEOS_SOCKET_DIR for custom path",
                petal_tongue_core::constants::socket_search_dirs().len(),
                biomeos_device_management_socket_name(),
                biomeos_ui_socket_name(),
                discovery_service_socket_name(),
                biomeos_legacy_socket_name()
            ),
        })
    }

    /// Get standard Unix socket paths to scan (DH-1: searches all tier dirs)
    #[doc(hidden)]
    pub fn get_standard_socket_paths() -> DiscoveryResult<Vec<PathBuf>> {
        let socket_names = [
            biomeos_device_management_socket_name(),
            biomeos_ui_socket_name(),
            discovery_service_socket_name(),
            biomeos_legacy_socket_name(),
        ];
        let mut paths = Vec::new();
        for search_dir in petal_tongue_core::constants::socket_search_dirs() {
            for name in &socket_names {
                paths.push(search_dir.join(format!("{name}.sock")));
            }
        }
        Ok(paths)
    }

    async fn test_connection(path: &Path) -> DiscoveryResult<()> {
        let endpoint = TransportEndpoint::uds(path);
        let stream = tokio::time::timeout(Duration::from_secs(2), connect_transport(&endpoint))
            .await
            .map_err(|_| DiscoveryError::ConnectionTimeout {
                endpoint: path.display().to_string(),
            })?
            .map_err(|e| {
                DiscoveryError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

        drop(stream);
        Ok(())
    }

    async fn call(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> DiscoveryResult<serde_json::Value> {
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);

        let request = JsonRpcRequest::new(method, params, id);

        debug!("→ JSON-RPC request: {} (id={})", method, id);

        let endpoint = TransportEndpoint::uds(&self.socket_path);
        let stream = tokio::time::timeout(self.timeout, connect_transport(&endpoint))
            .await
            .map_err(|_| DiscoveryError::ConnectionTimeout {
                endpoint: self.socket_path.display().to_string(),
            })?
            .map_err(|e| {
                DiscoveryError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            })?;

        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);

        let request_json = serde_json::to_string(&request).map_err(DiscoveryError::Json)? + "\n";
        writer
            .write_all(request_json.as_bytes())
            .await
            .map_err(DiscoveryError::Io)?;
        writer.flush().await.map_err(DiscoveryError::Io)?;

        debug!("✓ Request sent");

        let mut line = String::new();
        tokio::time::timeout(self.timeout, reader.read_line(&mut line))
            .await
            .map_err(|_| DiscoveryError::RpcTimeout {
                context: method.to_string(),
            })??;

        debug!("← JSON-RPC response ({} bytes)", line.len());

        let response: JsonRpcResponse =
            serde_json::from_str(&line).map_err(|e| DiscoveryError::ParseError {
                data_type: "JSON-RPC response".to_owned(),
                message: e.to_string(),
            })?;

        if response.id != serde_json::json!(id) {
            return Err(DiscoveryError::RequestIdMismatch {
                expected: id,
                actual: response.id,
            });
        }

        if let Some(error) = response.error {
            return Err(DiscoveryError::JsonRpcError {
                code: Some(error.code),
                message: format!(
                    "{}{}",
                    error.message,
                    error
                        .data
                        .map(|d| format!(" (data: {d})"))
                        .unwrap_or_default()
                ),
            });
        }

        Ok(response.result.unwrap_or(serde_json::Value::Null))
    }

    async fn call_with_retry(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
        max_retries: u32,
    ) -> DiscoveryResult<serde_json::Value> {
        let mut last_error = None;

        for attempt in 1..=max_retries {
            match self.call(method, params.clone()).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if e.to_string().contains("JSON-RPC error") {
                        return Err(e);
                    }

                    debug!(
                        "RPC call failed (attempt {}/{}): {}",
                        attempt, max_retries, e
                    );
                    last_error = Some(e);

                    if attempt < max_retries {
                        let delay_ms = 100 * (1 << (attempt - 1));
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or(DiscoveryError::RetryExhausted))
    }
}

impl VisualizationDataProvider for JsonRpcProvider {
    async fn get_primals(&self) -> DiscoveryResult<Vec<PrimalInfo>> {
        debug!("Calling primal.list via JSON-RPC");

        let result = self.call_with_retry("primal.list", None, 3).await?;

        let primals: Vec<PrimalInfo> =
            serde_json::from_value(result).map_err(|e| DiscoveryError::ParseError {
                data_type: "primals".to_owned(),
                message: e.to_string(),
            })?;

        debug!("✓ Received {} primals", primals.len());
        Ok(primals)
    }

    async fn get_topology(&self) -> DiscoveryResult<Vec<TopologyEdge>> {
        debug!("Calling topology.get via JSON-RPC");

        let result = self.call("topology.get", None).await;
        let result = match result {
            Ok(r) => Ok(r),
            Err(e)
                if e.to_string().contains("-32601")
                    || e.to_string().contains("Method not found") =>
            {
                self.call("get_topology", None).await
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(result) => {
                let topology: Vec<TopologyEdge> =
                    serde_json::from_value(result).map_err(|e| DiscoveryError::ParseError {
                        data_type: "topology".to_owned(),
                        message: e.to_string(),
                    })?;
                debug!("✓ Received {} edges", topology.len());
                Ok(topology)
            }
            Err(e) => {
                if e.to_string().contains("-32601") || e.to_string().contains("Method not found") {
                    debug!("Topology not supported by provider (graceful fallback)");
                    Ok(Vec::new())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn health_check(&self) -> DiscoveryResult<String> {
        debug!("Performing JSON-RPC health check");

        self.call("primal.list", None).await?;

        Ok(format!(
            "JSON-RPC provider at {} is healthy",
            self.socket_path.display()
        ))
    }

    fn get_metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            name: "JSON-RPC Provider".to_owned(),
            endpoint: format!("unix://{}", self.socket_path.display()),
            protocol: "jsonrpc-2.0".to_owned(),
            capabilities: vec![
                "primals".to_owned(),
                "devices".to_owned(),
                "topology".to_owned(),
            ],
        }
    }
}
