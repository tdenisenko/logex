use std::path::Path;
use std::time::Duration;

use futures_util::{StreamExt, stream::FuturesUnordered};
use logex_cl::MAINNET_CONSENSUS_CHAIN_SPEC;
use serde::Deserialize;
use thiserror::Error;

const CHECKPOINT_SYNC_TIMEOUT: Duration = Duration::from_secs(30);
const CHECKPOINT_SLOTS_PER_EPOCH: u64 = 32;
pub const DEFAULT_CHECKPOINT_SYNC_URL: &str = "https://mainnet.checkpoint.sigp.io";
pub const RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG: u64 = 256;

#[derive(Debug, Error)]
pub enum CheckpointSyncError {
    #[error("invalid checkpoint-sync URL {0}")]
    InvalidUrl(String),
    #[error("invalid checkpoint {0}")]
    InvalidCheckpoint(String),
    #[error("checkpoint-sync URL is required to fetch or validate a recent checkpoint")]
    MissingCheckpointSyncUrl,
    #[error("unsupported checkpoint descriptor format for {0}")]
    UnsupportedDescriptor(String),
    #[error("failed to read checkpoint descriptor {path}: {source}")]
    ReadDescriptor {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse checkpoint descriptor {path}: {message}")]
    ParseDescriptor { path: String, message: String },
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
    #[error("checkpoint-sync endpoint root response from {url} is malformed: missing root")]
    MissingRoot { url: String },
    #[error("checkpoint-sync endpoint returned non-numeric slot {slot:?} for {url}")]
    InvalidSlot { url: String, slot: String },
    #[error("checkpoint-sync endpoint returned non-numeric epoch {epoch:?} for {url}")]
    InvalidEpoch { url: String, epoch: String },
    #[error(
        "checkpoint-sync endpoint could not resolve finalized checkpoint from {base_url}: {failures}"
    )]
    FinalizedFallbacksFailed { base_url: String, failures: String },
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
    #[error(
        "checkpoint-sync sources did not reach quorum {required}/{total}; successful={successful}; failures={failures}"
    )]
    InsufficientQuorum {
        required: usize,
        total: usize,
        successful: usize,
        failures: String,
    },
    #[error(
        "checkpoint-sync sources disagreed; required quorum {required}/{total}; candidates={candidates}"
    )]
    SourceDisagreement {
        required: usize,
        total: usize,
        candidates: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlineCheckpoint {
    slot: Option<u64>,
    root: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedCheckpoint {
    checkpoint: InlineCheckpoint,
    original_value: String,
    preserve_original_value: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct CheckpointDescriptorForValidation {
    beacon_root: String,
    #[serde(default)]
    beacon_slot: Option<u64>,
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

#[derive(Debug, Clone, Deserialize)]
struct BeaconBlockResponse {
    data: BeaconBlockData,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconBlockData {
    message: BeaconBlockMessage,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconBlockMessage {
    slot: String,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconRootResponse {
    data: BeaconRootData,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconRootData {
    root: String,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconFinalityCheckpointsResponse {
    data: BeaconFinalityCheckpointsData,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconFinalityCheckpointsData {
    finalized: BeaconCheckpoint,
}

#[derive(Debug, Clone, Deserialize)]
struct BeaconCheckpoint {
    epoch: String,
    root: String,
}

impl BeaconFinalityCheckpointsResponse {
    fn finalized_header(self, url: &str) -> Result<BeaconHeader, CheckpointSyncError> {
        let epoch = self.data.finalized.epoch.parse::<u64>().map_err(|_| {
            CheckpointSyncError::InvalidEpoch {
                url: url.to_owned(),
                epoch: self.data.finalized.epoch,
            }
        })?;
        let slot = epoch
            .checked_mul(CHECKPOINT_SLOTS_PER_EPOCH)
            .ok_or_else(|| CheckpointSyncError::InvalidEpoch {
                url: url.to_owned(),
                epoch: epoch.to_string(),
            })?;
        if self.data.finalized.root.is_empty() {
            return Err(CheckpointSyncError::MissingRoot {
                url: url.to_owned(),
            });
        }
        Ok(BeaconHeader {
            slot,
            root: self.data.finalized.root,
        })
    }
}

pub async fn resolve_checkpoint(
    checkpoint: Option<String>,
    checkpoint_sync_url: Option<&str>,
) -> Result<Option<String>, CheckpointSyncError> {
    let checkpoint_sync_url =
        checkpoint_sync_url.ok_or(CheckpointSyncError::MissingCheckpointSyncUrl)?;

    let parsed_checkpoint = match checkpoint {
        Some(checkpoint) => Some(parse_checkpoint_for_validation(checkpoint)?),
        None => None,
    };

    let sources = CheckpointSyncSources::new(checkpoint_sync_url)?;
    let client = reqwest::Client::builder()
        .timeout(CHECKPOINT_SYNC_TIMEOUT)
        .build()
        .map_err(|source| CheckpointSyncError::Request {
            url: checkpoint_sync_url.to_owned(),
            source,
        })?;
    let finalized = sources.fetch_finalized_headers(&client).await?;
    let newest_finalized = finalized
        .iter()
        .map(|header| &header.header)
        .max_by_key(|header| header.slot)
        .expect("fetch_finalized_headers returns a quorum");

    match parsed_checkpoint {
        None => {
            let resolved = sources
                .resolve_fresh_checkpoint(&client, &finalized)
                .await?;
            let checkpoint = resolved.header.inline_checkpoint();
            tracing::info!(
                checkpoint = %checkpoint,
                sources = %resolved.sources.join(","),
                "resolved recent checkpoint from checkpoint-sync source quorum"
            );
            Ok(Some(checkpoint))
        }
        Some(parsed) => {
            let resolved = sources
                .resolve_requested_checkpoint(&client, &parsed.checkpoint)
                .await?;
            let requested_root = normalize_root(&parsed.checkpoint.root);
            if normalize_root(&resolved.header.root) != requested_root {
                return Err(CheckpointSyncError::RootMismatch {
                    requested_root,
                    resolved_root: resolved.header.root,
                    resolved_slot: resolved.header.slot,
                });
            }

            reject_stale_checkpoint(&resolved.header, newest_finalized)?;
            let checkpoint = resolved.header.inline_checkpoint();
            tracing::info!(
                checkpoint = %checkpoint,
                finalized_slot = newest_finalized.slot,
                sources = %resolved.sources.join(","),
                "validated recent checkpoint against checkpoint-sync source quorum"
            );
            if parsed.preserve_original_value {
                Ok(Some(parsed.original_value))
            } else {
                Ok(Some(checkpoint))
            }
        }
    }
}

fn reject_stale_checkpoint(
    checkpoint: &BeaconHeader,
    finalized: &BeaconHeader,
) -> Result<(), CheckpointSyncError> {
    let checkpoint_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(checkpoint.slot);
    let finalized_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(finalized.slot);
    if finalized_epoch > checkpoint_epoch.saturating_add(RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG)
    {
        return Err(CheckpointSyncError::Stale {
            slot: checkpoint.slot,
            root: checkpoint.root.clone(),
            finalized_slot: finalized.slot,
            max_epochs: RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG,
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

#[derive(Debug, Clone)]
struct SourceBeaconHeader {
    endpoint: String,
    header: BeaconHeader,
}

#[derive(Debug, Clone)]
struct ResolvedSourceCheckpoint {
    header: BeaconHeader,
    sources: Vec<String>,
}

struct CheckpointSyncSources {
    endpoints: Vec<CheckpointSyncEndpoint>,
}

impl CheckpointSyncSources {
    fn new(urls: &str) -> Result<Self, CheckpointSyncError> {
        let mut endpoints = Vec::new();
        for raw_url in urls.split(',') {
            let url = raw_url.trim();
            if url.is_empty() {
                continue;
            }
            let endpoint = CheckpointSyncEndpoint::new(url)?;
            if !endpoints
                .iter()
                .any(|existing: &CheckpointSyncEndpoint| existing.base_url == endpoint.base_url)
            {
                endpoints.push(endpoint);
            }
        }
        if endpoints.is_empty() {
            return Err(CheckpointSyncError::InvalidUrl(urls.to_owned()));
        }
        Ok(Self { endpoints })
    }

    fn quorum_threshold(&self) -> usize {
        if self.endpoints.len() == 1 {
            1
        } else {
            self.endpoints.len() / 2 + 1
        }
    }

    async fn fetch_finalized_headers(
        &self,
        client: &reqwest::Client,
    ) -> Result<Vec<SourceBeaconHeader>, CheckpointSyncError> {
        let outcomes = self.fetch_headers(client, "finalized").await;
        self.require_min_successes(outcomes)
    }

    async fn resolve_fresh_checkpoint(
        &self,
        client: &reqwest::Client,
        finalized: &[SourceBeaconHeader],
    ) -> Result<ResolvedSourceCheckpoint, CheckpointSyncError> {
        if self.endpoints.len() == 1 {
            return Ok(ResolvedSourceCheckpoint {
                header: finalized[0].header.clone(),
                sources: vec![finalized[0].endpoint.clone()],
            });
        }

        let lowest_finalized_slot = finalized
            .iter()
            .map(|header| header.header.slot)
            .min()
            .expect("fresh checkpoint resolution requires finalized headers");
        let outcomes = self
            .fetch_headers(client, &lowest_finalized_slot.to_string())
            .await;
        let headers = self.require_min_successes(outcomes)?;
        select_quorum_checkpoint(headers, self.quorum_threshold(), self.endpoints.len())
    }

    async fn resolve_requested_checkpoint(
        &self,
        client: &reqwest::Client,
        checkpoint: &InlineCheckpoint,
    ) -> Result<ResolvedSourceCheckpoint, CheckpointSyncError> {
        let block_id = checkpoint
            .slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| checkpoint.root.clone());
        let outcomes = self.fetch_headers(client, &block_id).await;
        let headers = self.require_min_successes(outcomes)?;
        let requested_root = normalize_root(&checkpoint.root);
        let matching = headers
            .iter()
            .filter(|header| normalize_root(&header.header.root) == requested_root)
            .cloned()
            .collect::<Vec<_>>();
        if matching.len() < self.quorum_threshold() {
            return Err(CheckpointSyncError::RootMismatch {
                requested_root,
                resolved_root: format_header_candidates(&headers),
                resolved_slot: checkpoint.slot.unwrap_or(0),
            });
        }
        select_quorum_checkpoint(matching, self.quorum_threshold(), self.endpoints.len())
    }

    async fn fetch_headers(
        &self,
        client: &reqwest::Client,
        block_id: &str,
    ) -> Vec<Result<SourceBeaconHeader, CheckpointSyncError>> {
        let mut requests = FuturesUnordered::new();
        for endpoint in &self.endpoints {
            requests.push(async move {
                let header = endpoint.fetch_header(client, block_id).await?;
                Ok(SourceBeaconHeader {
                    endpoint: endpoint.base_url.clone(),
                    header,
                })
            });
        }

        let mut outcomes = Vec::with_capacity(self.endpoints.len());
        while let Some(outcome) = requests.next().await {
            outcomes.push(outcome);
        }
        outcomes
    }

    fn require_min_successes(
        &self,
        outcomes: Vec<Result<SourceBeaconHeader, CheckpointSyncError>>,
    ) -> Result<Vec<SourceBeaconHeader>, CheckpointSyncError> {
        let mut headers = Vec::new();
        let mut failures = Vec::new();
        for outcome in outcomes {
            match outcome {
                Ok(header) => headers.push(header),
                Err(error) => failures.push(error.to_string()),
            }
        }
        let required = self.quorum_threshold();
        if headers.len() < required {
            return Err(CheckpointSyncError::InsufficientQuorum {
                required,
                total: self.endpoints.len(),
                successful: headers.len(),
                failures: failures.join("; "),
            });
        }
        Ok(headers)
    }
}

fn select_quorum_checkpoint(
    headers: Vec<SourceBeaconHeader>,
    required: usize,
    total: usize,
) -> Result<ResolvedSourceCheckpoint, CheckpointSyncError> {
    let mut groups: Vec<ResolvedSourceCheckpoint> = Vec::new();
    for header in headers {
        let root = normalize_root(&header.header.root);
        if let Some(group) = groups.iter_mut().find(|group| {
            group.header.slot == header.header.slot && normalize_root(&group.header.root) == root
        }) {
            group.sources.push(header.endpoint);
        } else {
            groups.push(ResolvedSourceCheckpoint {
                header: BeaconHeader {
                    slot: header.header.slot,
                    root,
                },
                sources: vec![header.endpoint],
            });
        }
    }

    groups.sort_by(|left, right| {
        right
            .sources
            .len()
            .cmp(&left.sources.len())
            .then_with(|| right.header.slot.cmp(&left.header.slot))
            .then_with(|| left.header.root.cmp(&right.header.root))
    });
    if let Some(group) = groups.first()
        && group.sources.len() >= required
    {
        return Ok(group.clone());
    }

    Err(CheckpointSyncError::SourceDisagreement {
        required,
        total,
        candidates: format_resolved_candidates(&groups),
    })
}

fn format_header_candidates(headers: &[SourceBeaconHeader]) -> String {
    headers
        .iter()
        .map(|header| {
            format!(
                "{}@{} from {}",
                header.header.slot,
                normalize_root(&header.header.root),
                header.endpoint
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_resolved_candidates(groups: &[ResolvedSourceCheckpoint]) -> String {
    groups
        .iter()
        .map(|group| {
            format!(
                "{}@{} from {} source(s): {}",
                group.header.slot,
                normalize_root(&group.header.root),
                group.sources.len(),
                group.sources.join(",")
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[derive(Debug, Clone)]
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
        if block_id == "finalized" {
            return self.fetch_finalized_header(client).await;
        }

        self.fetch_beacon_header(client, block_id).await
    }

    async fn fetch_finalized_header(
        &self,
        client: &reqwest::Client,
    ) -> Result<BeaconHeader, CheckpointSyncError> {
        let mut failures = Vec::new();

        match self.fetch_beacon_header(client, "finalized").await {
            Ok(header) => return Ok(header),
            Err(error) => failures.push(error.to_string()),
        }

        match self.fetch_finalized_block(client).await {
            Ok(header) => return Ok(header),
            Err(error) => failures.push(error.to_string()),
        }

        match self.fetch_finality_checkpoint(client).await {
            Ok(header) => return Ok(header),
            Err(error) => failures.push(error.to_string()),
        }

        Err(CheckpointSyncError::FinalizedFallbacksFailed {
            base_url: self.base_url.clone(),
            failures: failures.join("; "),
        })
    }

    async fn fetch_beacon_header(
        &self,
        client: &reqwest::Client,
        block_id: &str,
    ) -> Result<BeaconHeader, CheckpointSyncError> {
        let url = format!("{}/eth/v1/beacon/headers/{}", self.base_url, block_id);
        let response = get_checkpoint_response(client, &url).await?;
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

    async fn fetch_finalized_block(
        &self,
        client: &reqwest::Client,
    ) -> Result<BeaconHeader, CheckpointSyncError> {
        let block_url = format!("{}/eth/v2/beacon/blocks/finalized", self.base_url);
        let response = get_checkpoint_response(client, &block_url).await?;
        if !response.status().is_success() {
            return Err(CheckpointSyncError::HttpStatus {
                url: block_url,
                status: response.status(),
            });
        }
        let block = response
            .json::<BeaconBlockResponse>()
            .await
            .map_err(|source| CheckpointSyncError::Decode {
                url: block_url.clone(),
                source,
            })?;
        let slot =
            block
                .data
                .message
                .slot
                .parse()
                .map_err(|_| CheckpointSyncError::InvalidSlot {
                    url: block_url.clone(),
                    slot: block.data.message.slot,
                })?;

        let root_url = format!("{}/eth/v1/beacon/blocks/{slot}/root", self.base_url);
        let response = get_checkpoint_response(client, &root_url).await?;
        if !response.status().is_success() {
            return Err(CheckpointSyncError::HttpStatus {
                url: root_url,
                status: response.status(),
            });
        }
        let root = response
            .json::<BeaconRootResponse>()
            .await
            .map_err(|source| CheckpointSyncError::Decode {
                url: root_url.clone(),
                source,
            })?
            .data
            .root;
        if root.is_empty() {
            return Err(CheckpointSyncError::MissingRoot { url: root_url });
        }

        Ok(BeaconHeader { slot, root })
    }

    async fn fetch_finality_checkpoint(
        &self,
        client: &reqwest::Client,
    ) -> Result<BeaconHeader, CheckpointSyncError> {
        let url = format!(
            "{}/eth/v1/beacon/states/finalized/finality_checkpoints",
            self.base_url
        );
        let response = get_checkpoint_response(client, &url).await?;
        if !response.status().is_success() {
            return Err(CheckpointSyncError::HttpStatus {
                url,
                status: response.status(),
            });
        }
        response
            .json::<BeaconFinalityCheckpointsResponse>()
            .await
            .map_err(|source| CheckpointSyncError::Decode {
                url: url.clone(),
                source,
            })?
            .finalized_header(&url)
    }
}

async fn get_checkpoint_response(
    client: &reqwest::Client,
    url: &str,
) -> Result<reqwest::Response, CheckpointSyncError> {
    client
        .get(url)
        .send()
        .await
        .map_err(|source| CheckpointSyncError::Request {
            url: url.to_owned(),
            source,
        })
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

fn parse_checkpoint_for_validation(input: String) -> Result<ParsedCheckpoint, CheckpointSyncError> {
    if Path::new(&input).exists() {
        let checkpoint = parse_checkpoint_descriptor_for_validation(&input)?;
        return Ok(ParsedCheckpoint {
            checkpoint,
            original_value: input,
            preserve_original_value: true,
        });
    }

    let checkpoint = parse_inline_checkpoint(&input)?
        .ok_or_else(|| CheckpointSyncError::InvalidCheckpoint(input.clone()))?;
    Ok(ParsedCheckpoint {
        checkpoint,
        original_value: input,
        preserve_original_value: false,
    })
}

fn parse_checkpoint_descriptor_for_validation(
    path: &str,
) -> Result<InlineCheckpoint, CheckpointSyncError> {
    let contents =
        std::fs::read_to_string(path).map_err(|source| CheckpointSyncError::ReadDescriptor {
            path: path.to_owned(),
            source,
        })?;
    let descriptor =
        match Path::new(path).extension().and_then(|ext| ext.to_str()) {
            Some("json") => serde_json::from_str::<CheckpointDescriptorForValidation>(&contents)
                .map_err(|error| CheckpointSyncError::ParseDescriptor {
                    path: path.to_owned(),
                    message: error.to_string(),
                })?,
            Some("toml") => toml::from_str::<CheckpointDescriptorForValidation>(&contents)
                .map_err(|error| CheckpointSyncError::ParseDescriptor {
                    path: path.to_owned(),
                    message: error.to_string(),
                })?,
            _ => return Err(CheckpointSyncError::UnsupportedDescriptor(path.to_owned())),
        };
    Ok(InlineCheckpoint {
        slot: descriptor.beacon_slot,
        root: parse_checkpoint_root(path, &descriptor.beacon_root)?,
    })
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

    #[test]
    fn inline_checkpoint_is_normalized_after_validation() {
        assert_eq!(
            parse_checkpoint_for_validation(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()
            )
            .unwrap(),
            ParsedCheckpoint {
                checkpoint: InlineCheckpoint {
                    slot: None,
                    root: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                },
                original_value:
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                preserve_original_value: false,
            }
        );
    }

    #[tokio::test]
    async fn checkpoint_resolution_requires_validation_source() {
        assert!(matches!(
            resolve_checkpoint(
                Some(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()
                ),
                None,
            )
            .await,
            Err(CheckpointSyncError::MissingCheckpointSyncUrl)
        ));
    }

    #[test]
    fn descriptor_file_checkpoint_is_parsed_for_validation() {
        let temp = tempfile::tempdir().unwrap();
        let descriptor = temp.path().join("checkpoint.json");
        std::fs::write(
            &descriptor,
            r#"{
  "beacon_root": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "beacon_slot": 123
}"#,
        )
        .unwrap();

        assert_eq!(
            parse_checkpoint_for_validation(descriptor.to_string_lossy().to_string()).unwrap(),
            ParsedCheckpoint {
                checkpoint: InlineCheckpoint {
                    slot: Some(123),
                    root: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                },
                original_value: descriptor.to_string_lossy().to_string(),
                preserve_original_value: true,
            }
        );
    }

    #[test]
    fn parses_comma_separated_checkpoint_sources_and_quorum() {
        let sources = CheckpointSyncSources::new(
            " https://a.example/ ,https://b.example,,https://a.example ",
        )
        .unwrap();

        assert_eq!(sources.endpoints.len(), 2);
        assert_eq!(sources.endpoints[0].base_url, "https://a.example");
        assert_eq!(sources.endpoints[1].base_url, "https://b.example");
        assert_eq!(sources.quorum_threshold(), 2);

        let single = CheckpointSyncSources::new("https://a.example,https://a.example/").unwrap();
        assert_eq!(single.endpoints.len(), 1);
        assert_eq!(single.quorum_threshold(), 1);
    }

    #[test]
    fn checkpoint_source_quorum_selects_matching_candidate() {
        let selected = select_quorum_checkpoint(
            vec![
                source_header("https://a.example", 64, "0xaa"),
                source_header("https://b.example", 64, "0xaa"),
                source_header("https://c.example", 64, "0xbb"),
            ],
            2,
            3,
        )
        .unwrap();

        assert_eq!(selected.header.slot, 64);
        assert_eq!(selected.header.root, "0xaa");
        assert_eq!(
            selected.sources,
            vec![
                "https://a.example".to_owned(),
                "https://b.example".to_owned()
            ]
        );
    }

    #[test]
    fn checkpoint_source_quorum_rejects_disagreement() {
        assert!(matches!(
            select_quorum_checkpoint(
                vec![
                    source_header("https://a.example", 64, "0xaa"),
                    source_header("https://b.example", 64, "0xbb"),
                    source_header("https://c.example", 64, "0xcc"),
                ],
                2,
                3,
            ),
            Err(CheckpointSyncError::SourceDisagreement { .. })
        ));
    }

    #[test]
    fn rejects_checkpoint_older_than_endpoint_finality_window() {
        let checkpoint = BeaconHeader {
            slot: 32,
            root: "0x01".to_owned(),
        };
        let finalized = BeaconHeader {
            slot: (RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG + 2) * 32,
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

    #[test]
    fn parses_checkpoint_block_and_root_responses() {
        let block: BeaconBlockResponse =
            serde_json::from_str(r#"{"data":{"message":{"slot":"14279808","body":{}}}}"#).unwrap();
        let root: BeaconRootResponse = serde_json::from_str(
            r#"{"data":{"root":"0x925f664ef7716a6101e4713538688b30ef8661f225abcfb854f7e2221df1269c"}}"#,
        )
        .unwrap();

        assert_eq!(block.data.message.slot, "14279808");
        assert_eq!(
            root.data.root,
            "0x925f664ef7716a6101e4713538688b30ef8661f225abcfb854f7e2221df1269c"
        );
    }

    #[test]
    fn parses_finality_checkpoints_response_as_checkpoint_header() {
        let response: BeaconFinalityCheckpointsResponse = serde_json::from_str(
            r#"{"data":{"previous_justified":{"epoch":"458183","root":"0x01"},"current_justified":{"epoch":"458184","root":"0x02"},"finalized":{"epoch":"458184","root":"0x61e8d54d6fff34c1f77a541b113581be3c394aa393ce4bb8480ae1ed27b3e60f"}}}"#,
        )
        .unwrap();

        let header = response
            .finalized_header(
                "https://example.test/eth/v1/beacon/states/finalized/finality_checkpoints",
            )
            .unwrap();
        assert_eq!(header.slot, 458_184 * 32);
        assert_eq!(
            header.root,
            "0x61e8d54d6fff34c1f77a541b113581be3c394aa393ce4bb8480ae1ed27b3e60f"
        );
    }

    #[test]
    fn rejects_invalid_finality_checkpoint_epoch() {
        let response: BeaconFinalityCheckpointsResponse = serde_json::from_str(
            r#"{"data":{"finalized":{"epoch":"not-a-number","root":"0x61e8d54d6fff34c1f77a541b113581be3c394aa393ce4bb8480ae1ed27b3e60f"}}}"#,
        )
        .unwrap();

        assert!(matches!(
            response.finalized_header("https://example.test/finality"),
            Err(CheckpointSyncError::InvalidEpoch { .. })
        ));
    }

    #[test]
    fn rejects_empty_finality_checkpoint_root() {
        let response: BeaconFinalityCheckpointsResponse =
            serde_json::from_str(r#"{"data":{"finalized":{"epoch":"458184","root":""}}}"#).unwrap();

        assert!(matches!(
            response.finalized_header("https://example.test/finality"),
            Err(CheckpointSyncError::MissingRoot { .. })
        ));
    }

    fn source_header(endpoint: &str, slot: u64, root: &str) -> SourceBeaconHeader {
        SourceBeaconHeader {
            endpoint: endpoint.to_owned(),
            header: BeaconHeader {
                slot,
                root: root.to_owned(),
            },
        }
    }
}
