use std::path::Path;

use logex_cl::ConsensusStore;
use logex_index::IndexBuilder;
use logex_storage::{PartitionManager, PartitionManagerConfig};

pub fn run_build_indexes(config: PartitionManagerConfig) {
    let storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let hot_path = &storage.hot_partition().meta.path;
    if storage.hot_partition().meta.row_count == 0 {
        println!("Hot partition is empty, nothing to index");
        return;
    }

    tracing::info!(
        rows = storage.hot_partition().meta.row_count,
        "building indexes on hot partition"
    );
    if let Err(e) = IndexBuilder::build_all_indexes(hot_path) {
        tracing::error!(error = %e, "failed to build indexes");
        std::process::exit(1);
    }
    tracing::info!("indexes built successfully");
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
    }
}

fn consensus_state_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("cl").join("consensus_state.json")
}
