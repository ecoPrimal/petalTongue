// SPDX-License-Identifier: AGPL-3.0-or-later
//! [`BiomeOsBackend`]: socket discovery, capability query, WebSocket subscription.

use crate::capability_discovery::{
    CapabilityQuery, DiscoveryBackend, DiscoveryError, PrimalEndpoint,
};
use tokio::sync::mpsc;

use super::client::BiomeOsClient;
use super::types::{BiomeOSDiscoveryEvent, BiomeOsPrimal, JsonRpcRequest};

/// Error during WebSocket topology subscription.
#[derive(Debug, thiserror::Error)]
enum WebSocketBridgeError {
    /// Failed to establish the WebSocket connection.
    #[error("WebSocket connect failed: {0}")]
    Connect(tokio_tungstenite::tungstenite::Error),
    /// WebSocket protocol error during message receive.
    #[error("WebSocket error: {0}")]
    Protocol(tokio_tungstenite::tungstenite::Error),
    /// Server sent a close frame.
    #[error("WebSocket closed by server")]
    ClosedByServer,
    /// The WebSocket stream ended without a close frame.
    #[error("WebSocket stream ended")]
    StreamEnded,
}

/// biomeOS discovery backend
#[derive(Debug)]
pub struct BiomeOsBackend {
    /// JSON-RPC client for biomeOS Neural API
    client: BiomeOsClient,
}

impl BiomeOsBackend {
    /// Create a new biomeOS discovery backend from a socket path.
    pub fn new(socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            client: BiomeOsClient::from_socket_path(socket_path),
        }
    }

    /// Try to create from environment (`XDG_RUNTIME_DIR` or fallback)
    /// Create from environment with capability-based discovery
    ///
    /// # Socket Discovery Priority
    /// 1. `BIOMEOS_NEURAL_API_SOCKET` - explicit override (highest priority)
    /// 2. `$XDG_RUNTIME_DIR/biomeos/neural-api.sock` - XDG standard
    /// 3. `/tmp/biomeos-neural-api.sock` - legacy fallback
    ///
    /// # TRUE PRIMAL: Zero hardcoded paths in discovery logic
    ///
    /// # Errors
    ///
    /// Returns an error if no biomeOS Neural API socket is found in any discovery
    /// path (`BIOMEOS_NEURAL_API_SOCKET` env var, XDG runtime dir, or legacy `/tmp` fallback).
    pub fn from_env() -> Result<Self, DiscoveryError> {
        use crate::platform_dirs;

        if let Ok(socket_path) = std::env::var(crate::constants::BIOMEOS_NEURAL_API_SOCKET) {
            let path = std::path::PathBuf::from(&socket_path);
            if path.exists() {
                return Ok(Self::new(socket_path));
            }
            tracing::debug!(
                "BIOMEOS_NEURAL_API_SOCKET={} but socket not found, trying discovery",
                socket_path
            );
        }

        if let Ok(runtime_dir) = platform_dirs::runtime_dir() {
            let socket_path =
                runtime_dir.join(format!("{}.sock", crate::constants::biomeos_socket_name()));
            if socket_path.exists() {
                return Ok(Self::new(socket_path.to_string_lossy().into_owned()));
            }
        }

        let fallback = crate::constants::biomeos_legacy_socket();
        if fallback.exists() {
            return Ok(Self::new(fallback.to_string_lossy().into_owned()));
        }

        Err(DiscoveryError::BackendUnavailable(
            "biomeOS Neural API socket not found. Set BIOMEOS_NEURAL_API_SOCKET env var or start biomeOS.".to_owned(),
        ))
    }

    /// Subscribe to real-time topology/health updates via WebSocket.
    ///
    /// Connects to the biomeOS WebSocket endpoint (discovered via socket/env),
    /// subscribes to topology and health events, and returns a receiver stream.
    /// Handles reconnection gracefully by spawning a background task that
    /// retries on disconnect.
    ///
    /// # Errors
    ///
    /// Never returns an error; always returns `Ok` with a receiver. The WebSocket
    /// connection runs in a background task.
    #[expect(
        clippy::unused_async,
        reason = "async for API consistency with other discovery methods"
    )]
    pub async fn subscribe_websocket(
        &self,
        _query: &CapabilityQuery,
    ) -> Result<mpsc::Receiver<BiomeOSDiscoveryEvent>, DiscoveryError> {
        let ws_url = Self::derive_websocket_url();
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(Self::websocket_loop(ws_url, tx));
        Ok(rx)
    }

    fn derive_websocket_url() -> String {
        if let Ok(url) = std::env::var(crate::constants::BIOMEOS_WS_ENDPOINT) {
            return url;
        }
        crate::constants::default_biomeos_ws_topology_url()
    }

    async fn websocket_loop(url: String, tx: mpsc::Sender<BiomeOSDiscoveryEvent>) {
        const MAX_BACKOFF: u64 = 30;
        let mut backoff = 1u64;

        loop {
            match Self::connect_and_forward(&url, &tx).await {
                Ok(()) => {
                    backoff = 1;
                }
                Err(e) => {
                    tracing::debug!("WebSocket disconnected: {e}, reconnecting in {backoff}s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    async fn connect_and_forward(
        url: &str,
        tx: &mpsc::Sender<BiomeOSDiscoveryEvent>,
    ) -> Result<(), WebSocketBridgeError> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::connect_async;

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(WebSocketBridgeError::Connect)?;

        let (mut write, mut read) = ws_stream.split();

        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "topology.subscribe",
            "params": {},
            "id": 1
        });
        let _ = write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_msg.to_string().into(),
            ))
            .await;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    if let Ok(event) = serde_json::from_str::<BiomeOSDiscoveryEvent>(&text)
                        && tx.send(event).await.is_err()
                    {
                        return Ok(());
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                    return Err(WebSocketBridgeError::ClosedByServer);
                }
                Err(e) => return Err(WebSocketBridgeError::Protocol(e)),
                _ => {}
            }
        }
        Err(WebSocketBridgeError::StreamEnded)
    }
}

impl DiscoveryBackend for BiomeOsBackend {
    async fn query(&self, query: &CapabilityQuery) -> Result<Vec<PrimalEndpoint>, DiscoveryError> {
        let query = query.clone();
        let client = self.client.clone();
        let request = JsonRpcRequest::new(
            "discovery.query_capability",
            Some(serde_json::json!({
                "domain": query.domain,
                "operation": query.operation,
                "version_req": query.version_req,
            })),
            1,
        );

        let response = client
            .call(&request)
            .await
            .map_err(|e| DiscoveryError::CommunicationError(e.to_string()))?;

        if let Some(error) = response.error {
            if error.message.contains("not found") {
                return Err(DiscoveryError::CapabilityNotFound {
                    domain: query.domain.clone(),
                });
            }
            return Err(DiscoveryError::CommunicationError(error.message));
        }

        let result = response.result.ok_or_else(|| {
            DiscoveryError::CommunicationError("No result in response".to_owned())
        })?;

        let primals: Vec<BiomeOsPrimal> = serde_json::from_value(result)
            .map_err(|e| DiscoveryError::CommunicationError(format!("Parse error: {e}")))?;

        Ok(primals.into_iter().map(std::convert::Into::into).collect())
    }

    async fn subscribe(&self, _query: &CapabilityQuery) -> Result<(), DiscoveryError> {
        Ok(())
    }
}
