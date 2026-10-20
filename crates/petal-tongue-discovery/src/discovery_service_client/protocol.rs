// SPDX-License-Identifier: AGPL-3.0-or-later
//! JSON-RPC wire protocol: request/response framing and socket I/O.
//!
//! JSON-RPC 2.0 types come from `cellmembrane-types` (canonical source).

pub(super) use cellmembrane_types::{JsonRpcError, JsonRpcResponse};
use crate::errors::{DiscoveryError, DiscoveryResult};
use petal_tongue_core::transport::{TransportEndpoint, connect_transport};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::DiscoveryServiceClient;

impl DiscoveryServiceClient {
    /// Send JSON-RPC request to the discovery service
    ///
    /// Uses aggressive timeouts to prevent hanging on an unresponsive peer.
    pub(super) async fn send_request(&self, request: Value) -> DiscoveryResult<Value> {
        use petal_tongue_core::constants::discovery_timeouts;
        let connect_timeout = discovery_timeouts::DISCOVERY_SERVICE_CONNECT_TIMEOUT;

        let endpoint = TransportEndpoint::uds(&self.socket_path);
        let stream = match tokio::time::timeout(connect_timeout, connect_transport(&endpoint)).await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                return Err(DiscoveryError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                )));
            }
            Err(_) => {
                return Err(DiscoveryError::ConnectionTimeout {
                    endpoint: self.socket_path.display().to_string(),
                });
            }
        };

        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);

        let write_timeout = discovery_timeouts::DISCOVERY_SERVICE_WRITE_TIMEOUT;

        let mut request_bytes = serde_json::to_vec(&request).map_err(DiscoveryError::Json)?;
        request_bytes.push(b'\n');

        match tokio::time::timeout(write_timeout, async {
            writer.write_all(&request_bytes).await?;
            writer.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(DiscoveryError::Io(e)),
            Err(_) => {
                return Err(DiscoveryError::WriteTimeout {
                    endpoint: "discovery service".to_owned(),
                });
            }
        }

        let read_timeout = discovery_timeouts::DISCOVERY_SERVICE_READ_TIMEOUT;

        let mut line = String::new();
        match tokio::time::timeout(read_timeout, reader.read_line(&mut line)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(DiscoveryError::Io(e)),
            Err(_) => {
                return Err(DiscoveryError::ReadTimeout {
                    endpoint: "discovery service".to_owned(),
                });
            }
        }

        let response: JsonRpcResponse =
            serde_json::from_str(&line).map_err(DiscoveryError::Json)?;

        if let Some(error) = response.error {
            return Err(DiscoveryError::JsonRpcError {
                code: Some(error.code),
                message: error.message,
            });
        }

        response
            .result
            .ok_or_else(|| DiscoveryError::NoResultInResponse {
                context: " (discovery service)".to_owned(),
            })
    }
}
