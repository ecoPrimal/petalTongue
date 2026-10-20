// SPDX-License-Identifier: AGPL-3.0-or-later
//! JSON-RPC provider types
//!
//! JSON-RPC 2.0 wire types are re-exported from `cellmembrane-types` (canonical source).

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

// Canonical JSON-RPC 2.0 types from cellmembrane-types
pub use cellmembrane_types::{JsonRpcRequest, JsonRpcResponse};
// JsonRpcError is accessed transitively via JsonRpcResponse.error field

/// JSON-RPC 2.0 provider for Unix socket communication
pub struct JsonRpcProvider {
    pub(crate) socket_path: PathBuf,
    pub(crate) request_id: AtomicU64,
    pub(crate) timeout: Duration,
}
