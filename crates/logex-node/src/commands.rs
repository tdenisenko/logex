use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::cli::IndexProfile;
use logex_cl::ConsensusStore;
use logex_index::{IndexBuildProfile, IndexBuilder};
use logex_storage::{PartitionManager, PartitionManagerConfig};

#[derive(Debug, Clone, Copy)]
pub struct BuildIndexesOptions {
    pub sealed: bool,
    pub hot: bool,
    pub profile: IndexProfile,
    pub missing_only: bool,
    pub limit: Option<usize>,
    pub jobs: usize,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_timestamp: Option<u64>,
    pub to_timestamp: Option<u64>,
}

#[derive(Debug, Clone)]
struct IndexTarget {
    segment_id: u64,
    path: PathBuf,
    row_count: u64,
    min_block: u64,
    max_block: u64,
    min_timestamp: Option<u64>,
    max_timestamp: Option<u64>,
    kind: &'static str,
}

pub fn run_build_indexes(config: PartitionManagerConfig, options: BuildIndexesOptions) {
    let mut storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let profile = match options.profile {
        IndexProfile::All => IndexBuildProfile::All,
        IndexProfile::LogQuery => IndexBuildProfile::LogQuery,
        IndexProfile::Erc20Transfer => IndexBuildProfile::Erc20Transfer,
    };
    let mut targets = index_targets(&storage, options);
    if options.missing_only {
        targets.retain(|target| index_files_missing(&target.path, profile));
    }
    if let Some(limit) = options.limit {
        targets.truncate(limit);
    }

    if targets.is_empty() {
        println!("No matching segments need indexing");
        return;
    }

    let indexed_segments = match build_index_targets(&targets, profile, options) {
        Ok(indexed) => indexed,
        Err((target, error)) => {
            tracing::error!(
                error = %error,
                segment_id = target.segment_id,
                "failed to build indexes"
            );
            std::process::exit(1);
        }
    };

    for target in &indexed_segments {
        if let Err(e) = storage.refresh_segment_manifest(target.segment_id) {
            tracing::error!(
                error = %e,
                segment_id = target.segment_id,
                "failed to refresh segment manifest after index build"
            );
            std::process::exit(1);
        }
    }
    let indexed = indexed_segments.len();
    println!("Indexed {indexed} segment(s)");
}

fn build_index_targets(
    targets: &[IndexTarget],
    profile: IndexBuildProfile,
    options: BuildIndexesOptions,
) -> Result<Vec<IndexTarget>, (IndexTarget, std::io::Error)> {
    let jobs = options.jobs.max(1).min(targets.len()).min(
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
    );
    if jobs <= 1 {
        let mut indexed = Vec::with_capacity(targets.len());
        for target in targets {
            build_index_target(target, profile, options.missing_only)?;
            indexed.push(target.clone());
        }
        return Ok(indexed);
    }

    let targets = Arc::new(targets.to_vec());
    let next_index = Arc::new(AtomicUsize::new(0));
    let indexed = Arc::new(Mutex::new(Vec::with_capacity(targets.len())));
    let error = Arc::new(Mutex::new(None::<(IndexTarget, std::io::Error)>));

    std::thread::scope(|scope| {
        for _ in 0..jobs {
            let targets = Arc::clone(&targets);
            let next_index = Arc::clone(&next_index);
            let indexed = Arc::clone(&indexed);
            let error = Arc::clone(&error);
            scope.spawn(move || {
                loop {
                    if error.lock().expect("index error mutex poisoned").is_some() {
                        break;
                    }
                    let index = next_index.fetch_add(1, Ordering::Relaxed);
                    let Some(target) = targets.get(index) else {
                        break;
                    };
                    match build_index_target(target, profile, options.missing_only) {
                        Ok(()) => indexed
                            .lock()
                            .expect("indexed segments mutex poisoned")
                            .push(target.clone()),
                        Err((_, err)) => {
                            *error.lock().expect("index error mutex poisoned") =
                                Some((target.clone(), err));
                            break;
                        }
                    }
                }
            });
        }
    });

    if let Some(error) = error.lock().expect("index error mutex poisoned").take() {
        return Err(error);
    }
    Ok(Arc::try_unwrap(indexed)
        .expect("all index worker references should be dropped")
        .into_inner()
        .expect("indexed segments mutex poisoned"))
}

fn build_index_target(
    target: &IndexTarget,
    profile: IndexBuildProfile,
    missing_only: bool,
) -> Result<(), (IndexTarget, std::io::Error)> {
    tracing::info!(
        segment_id = target.segment_id,
        kind = target.kind,
        rows = target.row_count,
        min_block = target.min_block,
        max_block = target.max_block,
        min_timestamp = target.min_timestamp,
        max_timestamp = target.max_timestamp,
        profile = ?profile,
        "building segment indexes"
    );
    let result = if missing_only {
        IndexBuilder::build_missing_indexes(&target.path, profile)
    } else {
        IndexBuilder::build_indexes(&target.path, profile)
    };
    result.map_err(|error| (target.clone(), error))
}

fn index_targets(storage: &PartitionManager, options: BuildIndexesOptions) -> Vec<IndexTarget> {
    let include_hot = options.hot || !options.sealed;
    let mut targets = Vec::new();

    if options.sealed {
        targets.extend(
            storage
                .sealed_partitions()
                .iter()
                .filter(|partition| segment_matches_filters(&partition.meta, options))
                .map(|partition| IndexTarget {
                    segment_id: partition.meta.id,
                    path: partition.meta.path.clone(),
                    row_count: partition.meta.row_count,
                    min_block: partition.meta.min_block,
                    max_block: partition.meta.max_block,
                    min_timestamp: partition.meta.min_timestamp,
                    max_timestamp: partition.meta.max_timestamp,
                    kind: "sealed",
                }),
        );
    }

    if include_hot {
        let hot = storage.hot_partition();
        if segment_matches_filters(&hot.meta, options) {
            targets.push(IndexTarget {
                segment_id: hot.meta.id,
                path: hot.meta.path.clone(),
                row_count: hot.meta.row_count,
                min_block: hot.meta.min_block,
                max_block: hot.meta.max_block,
                min_timestamp: hot.meta.min_timestamp,
                max_timestamp: hot.meta.max_timestamp,
                kind: "hot",
            });
        }
    }

    targets.retain(|target| target.row_count > 0);
    targets
}

fn segment_matches_filters(
    meta: &logex_types::PartitionMeta,
    options: BuildIndexesOptions,
) -> bool {
    if let Some(from_block) = options.from_block
        && meta.max_block < from_block
    {
        return false;
    }
    if let Some(to_block) = options.to_block
        && meta.min_block > to_block
    {
        return false;
    }
    if let Some(from_timestamp) = options.from_timestamp
        && meta.max_timestamp.is_some_and(|max| max < from_timestamp)
    {
        return false;
    }
    if let Some(to_timestamp) = options.to_timestamp
        && meta.min_timestamp.is_some_and(|min| min > to_timestamp)
    {
        return false;
    }
    true
}

fn index_files_missing(path: &Path, profile: IndexBuildProfile) -> bool {
    let index_dir = path.join("indexes");
    IndexBuilder::required_index_files(profile)
        .iter()
        .any(|file_name| !index_dir.join(file_name).is_file())
}

pub fn run_compact(config: PartitionManagerConfig, limit: Option<usize>) {
    let mut storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let compacted = match limit {
        Some(limit) => storage.compact_eligible_segments_limit(limit),
        None => storage.compact_eligible_segments(),
    };
    match compacted {
        Ok(count) => println!("Compacted {count} sealed segments"),
        Err(e) => {
            tracing::error!(error = %e, "failed to compact sealed segments");
            std::process::exit(1);
        }
    }
}

pub fn run_info(config: PartitionManagerConfig) {
    let consensus_state_path = consensus_state_path(&config.data_dir);
    let consensus = if consensus_state_path.exists() {
        ConsensusStore::open(&config.data_dir, None).ok()
    } else {
        None
    };
    let storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    println!("LogEx Storage Info");
    println!("  Total rows:         {}", storage.total_rows());
    println!("  Sealed partitions:  {}", storage.sealed_count());
    println!(
        "  Synced head:        {}",
        storage
            .head_block()
            .map_or("none".to_string(), |b| b.to_string())
    );
    println!(
        "  Indexed head:       {}",
        storage
            .indexed_head_block()
            .map_or("none".to_string(), |b| b.to_string())
    );
    println!(
        "  Hot partition rows:  {}",
        storage.hot_partition().meta.row_count
    );
    if let Some(consensus) = consensus {
        let checkpoint = consensus.checkpoint();
        let anchors = consensus.chain_anchors();
        let light_client = consensus.light_client_status();
        println!("  Checkpoint root:     {}", checkpoint.beacon_root);
        println!(
            "  Checkpoint slot:     {}",
            checkpoint
                .beacon_slot
                .map_or("none".to_string(), |slot| slot.to_string())
        );
        println!(
            "  Optimistic head:     {}",
            anchors
                .optimistic_head
                .map_or("none".to_string(), |anchor| anchor.block_number.to_string())
        );
        println!(
            "  Finalized head:      {}",
            anchors
                .finalized_head
                .map_or("none".to_string(), |anchor| anchor.block_number.to_string())
        );
        if let Some(bootstrap) = light_client.bootstrap {
            println!(
                "  CL bootstrap:        {:?} @ slot {}",
                bootstrap.fork, bootstrap.header.beacon_slot
            );
        }
        if let Some(finality) = light_client.finality_update {
            println!(
                "  CL finality:         {:?} @ slot {}",
                finality.fork, finality.finalized_header.beacon_slot
            );
        }
        if let Some(optimistic) = light_client.optimistic_update {
            println!(
                "  CL optimistic:       {:?} @ slot {}",
                optimistic.fork, optimistic.attested_header.beacon_slot
            );
        }
    }
}

fn consensus_state_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("cl").join("consensus_state.json")
}
