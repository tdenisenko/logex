use std::path::Path;
use std::time::Duration;

use logex_cl::{CONSERVATIVE_WEAK_SUBJECTIVITY_FRESHNESS_EPOCHS, MAINNET_CONSENSUS_CHAIN_SPEC};
use serde::Deserialize;
use thiserror::Error;

const CHECKPOINT_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum CheckpointSyncError {
    #[error("invalid checkpoint-sync URL {0}")]
    InvalidUrl(String),
    #[error("invalid checkpoint {0}")]
    InvalidCheckpoint(String),
    #[error("failed to request {url}: {source}")]
    Request {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("checkpoint-sync endpoint returned HTTP {status} for {url}")]
    HttpStatus {
        url: String,
        status: reqwest::StatusCode,
    },
    #[error("checkpoint-sync endpoint response from {url} is malformed: {source}")]
    Decode {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("checkpoint-sync endpoint returned non-numeric slot {slot:?} for {url}")]
    InvalidSlot { url: String, slot: String },
    #[error(
        "checkpoint {slot}@{root} is stale relative to endpoint finalized slot {finalized_slot}; max freshness is {max_epochs} epochs"
    )]
    Stale {
        slot: u64,
        root: String,
        finalized_slot: u64,
        max_epochs: u64,
    },
    #[error(
        "checkpoint endpoint resolved root {resolved_root} for slot {resolved_slot}, but user supplied root {requested_root}"
    )]
    RootMismatch {
        requested_root: String,
        resolved_root: String,
        resolved_slot: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlineCheckpoint {
    slot: Option<u64>,
    root: String,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconHeaderResponse {
    data: BeaconHeaderData,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconHeaderData {
    root: String,
    header: SignedBeaconHeader,
}

#[derive(Debug, Clone, Deserialize)]
struct SignedBeaconHeader {
    message: BeaconHeaderMessage,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconHeaderMessage {
    slot: String,
}

pub async fn resolve_checkpoint(
    checkpoint: Option<String>,
    checkpoint_sync_url: Option<&str>,
) -> Result<Option<String>, CheckpointSyncError> {
    let Some(checkpoint_sync_url) = checkpoint_sync_url else {
        return Ok(checkpoint);
    };

    let parsed_checkpoint = match checkpoint.as_deref() {
        Some(checkpoint) => parse_inline_checkpoint(checkpoint)?,
        None => None,
    };
    if checkpoint.is_some() && parsed_checkpoint.is_none() {
        tracing::debug!(
            checkpoint = checkpoint.as_deref().unwrap_or_default(),
            source = %checkpoint_sync_url,
            "skipping checkpoint-sync endpoint validation for descriptor-file checkpoint"
        );
        return Ok(checkpoint);
    }

    let client = reqwest::Client::builder()
        .timeout(CHECKPOINT_SYNC_TIMEOUT)
        .build()
        .map_err(|source| CheckpointSyncError::Request {
            url: checkpoint_sync_url.to_owned(),
            source,
        })?;
    let endpoint = CheckpointSyncEndpoint::new(checkpoint_sync_url)?;
    let finalized = endpoint.fetch_header(&client, "finalized").await?;

    match checkpoint {
        None => {
            let checkpoint = finalized.inline_checkpoint();
            tracing::info!(
                checkpoint = %checkpoint,
                source = %endpoint.base_url,
                "resolved weak-subjectivity checkpoint from checkpoint-sync endpoint"
            );
            Ok(Some(checkpoint))
        }
        Some(_) => {
            let parsed = parsed_checkpoint.expect("inline checkpoint parsed before endpoint fetch");

            let resolved = match parsed.slot {
                Some(slot) => endpoint.fetch_header(&client, &slot.to_string()).await?,
                None => endpoint.fetch_header(&client, &parsed.root).await?,
            };
            let requested_root = normalize_root(&parsed.root);
            if normalize_root(&resolved.root) != requested_root {
                return Err(CheckpointSyncError::RootMismatch {
                    requested_root,
                    resolved_root: resolved.root,
                    resolved_slot: resolved.slot,
                });
            }

            reject_stale_checkpoint(&resolved, &finalized)?;
            let checkpoint = resolved.inline_checkpoint();
            tracing::info!(
                checkpoint = %checkpoint,
                finalized_slot = finalized.slot,
                source = %endpoint.base_url,
                "validated weak-subjectivity checkpoint against checkpoint-sync endpoint"
            );
            Ok(Some(checkpoint))
        }
    }
}

fn reject_stale_checkpoint(
    checkpoint: &BeaconHeader,
    finalized: &BeaconHeader,
) -> Result<(), CheckpointSyncError> {
    let checkpoint_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(checkpoint.slot);
    let finalized_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(finalized.slot);
    if finalized_epoch
        > checkpoint_epoch.saturating_add(CONSERVATIVE_WEAK_SUBJECTIVITY_FRESHNESS_EPOCHS)
    {
        return Err(CheckpointSyncError::Stale {
            slot: checkpoint.slot,
            root: checkpoint.root.clone(),
            finalized_slot: finalized.slot,
            max_epochs: CONSERVATIVE_WEAK_SUBJECTIVITY_FRESHNESS_EPOCHS,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BeaconHeader {
    slot: u64,
    root: String,
}

impl BeaconHeader {
    fn inline_checkpoint(&self) -> String {
        format!("{}@{}", self.slot, normalize_root(&self.root))
    }
}

struct CheckpointSyncEndpoint {
    base_url: String,
}

impl CheckpointSyncEndpoint {
    fn new(url: &str) -> Result<Self, CheckpointSyncError> {
        let base_url = url.trim().trim_end_matches('/');
        if base_url.is_empty() {
            return Err(CheckpointSyncError::InvalidUrl(url.to_owned()));
        }
        Ok(Self {
            base_url: base_url.to_owned(),
        })
    }

    async fn fetch_header(
        &self,
        client: &reqwest::Client,
        block_id: &str,
    ) -> Result<BeaconHeader, CheckpointSyncError> {
        let url = format!("{}/eth/v1/beacon/headers/{}", self.base_url, block_id);
        let response =
            client
                .get(&url)
                .send()
                .await
                .map_err(|source| CheckpointSyncError::Request {
                    url: url.clone(),
                    source,
                })?;
        if !response.status().is_success() {
            return Err(CheckpointSyncError::HttpStatus {
                url,
                status: response.status(),
            });
        }
        let header = response
            .json::<BeaconHeaderResponse>()
            .await
            .map_err(|source| CheckpointSyncError::Decode {
                url: url.clone(),
                source,
            })?;
        let slot = header.data.header.message.slot.parse().map_err(|_| {
            CheckpointSyncError::InvalidSlot {
                url: url.clone(),
                slot: header.data.header.message.slot,
            }
        })?;
        Ok(BeaconHeader {
            slot,
            root: header.data.root,
        })
    }
}

fn parse_inline_checkpoint(input: &str) -> Result<Option<InlineCheckpoint>, CheckpointSyncError> {
    if Path::new(input).exists() {
        return Ok(None);
    }
    if let Some((slot, root)) = input.split_once('@') {
        let slot = slot
            .parse()
            .map_err(|_| CheckpointSyncError::InvalidCheckpoint(input.to_owned()))?;
        let root = parse_checkpoint_root(input, root)?;
        return Ok(Some(InlineCheckpoint {
            slot: Some(slot),
            root,
        }));
    }
    Ok(Some(InlineCheckpoint {
        slot: None,
        root: parse_checkpoint_root(input, input)?,
    }))
}

fn parse_checkpoint_root(checkpoint: &str, root: &str) -> Result<String, CheckpointSyncError> {
    let root = normalize_root(root);
    if !is_normalized_checkpoint_root(&root) {
        return Err(CheckpointSyncError::InvalidCheckpoint(
            checkpoint.to_owned(),
        ));
    }
    Ok(root)
}

fn is_normalized_checkpoint_root(root: &str) -> bool {
    let Some(hex) = root.strip_prefix("0x") else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn normalize_root(root: &str) -> String {
    let root = root.trim();
    if root.starts_with("0x") {
        root.to_ascii_lowercase()
    } else {
        format!("0x{}", root.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inline_checkpoint_shapes() {
        assert_eq!(
            parse_inline_checkpoint(
                "123@0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
            .unwrap(),
            Some(InlineCheckpoint {
                slot: Some(123),
                root: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            })
        );
        assert_eq!(
            parse_inline_checkpoint(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
            .unwrap(),
            Some(InlineCheckpoint {
                slot: None,
                root: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            })
        );
        assert_eq!(
            parse_inline_checkpoint(
                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
            .unwrap(),
            Some(InlineCheckpoint {
                slot: None,
                root: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            })
        );
    }

    #[test]
    fn rejects_invalid_slot_in_inline_checkpoint() {
        assert!(matches!(
            parse_inline_checkpoint(
                "abc@0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            Err(CheckpointSyncError::InvalidCheckpoint(_))
        ));
    }

    #[test]
    fn rejects_invalid_root_in_inline_checkpoint() {
        assert!(matches!(
            parse_inline_checkpoint("123@0xaaaa"),
            Err(CheckpointSyncError::InvalidCheckpoint(_))
        ));
        assert!(matches!(
            parse_inline_checkpoint("not-a-checkpoint"),
            Err(CheckpointSyncError::InvalidCheckpoint(_))
        ));
    }

    #[tokio::test]
    async fn descriptor_file_checkpoint_skips_endpoint_resolution() {
        let checkpoint = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("Cargo.toml")
            .to_string_lossy()
            .to_string();

        assert_eq!(
            resolve_checkpoint(Some(checkpoint.clone()), Some("not a url"))
                .await
                .unwrap(),
            Some(checkpoint)
        );
    }

    #[test]
    fn rejects_checkpoint_older_than_endpoint_finality_window() {
        let checkpoint = BeaconHeader {
            slot: 32,
            root: "0x01".to_owned(),
        };
        let finalized = BeaconHeader {
            slot: (CONSERVATIVE_WEAK_SUBJECTIVITY_FRESHNESS_EPOCHS + 2) * 32,
            root: "0x02".to_owned(),
        };

        assert!(matches!(
            reject_stale_checkpoint(&checkpoint, &finalized),
            Err(CheckpointSyncError::Stale { .. })
        ));
    }

    #[test]
    fn formats_resolved_checkpoint_with_slot_and_root() {
        let header = BeaconHeader {
            slot: 42,
            root: "ABCDEF".to_owned(),
        };

        assert_eq!(header.inline_checkpoint(), "42@0xabcdef");
    }
}
