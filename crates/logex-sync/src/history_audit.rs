//! Explicit, bounded receipt audit inputs, separate from ingestion and queries.
//!
//! Local digests identify every selected physical occurrence. They do not prove
//! Ethereum completeness until compared with an authenticated receipt traversal.
//! All results remain provisional while the captured storage view can change.

mod comparison;
mod manifest;
mod network;

pub use comparison::{ReceiptComparison, ReceiptComparisonReport};
pub use manifest::{AuditManifest, AuditManifestLimits, AuditManifestSummary, AuditRange};

pub use network::{AuditNetworkClient, AuditNetworkService};
