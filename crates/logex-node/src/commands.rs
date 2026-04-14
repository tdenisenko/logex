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
}
