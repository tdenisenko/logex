#![feature(core_io_borrowed_buf, read_buf)]

pub mod bitmap;
mod bloom;
mod btree;
mod builder;
mod composite;
mod event_bloom;
mod index_file;
mod query_bitmap;
mod transfer_bloom;
mod verification;

pub use btree::{BTreeIndex, BTreeIndexReader};
pub use builder::{
    CapturedIndexVerification, IndexBuildProfile, IndexBuilder, IndexVerificationError,
    VerifiedIndexArtifact,
};
pub use composite::{CompositeIndexBuilder, CompositeQuery};
pub use event_bloom::{EVENT_BLOOM_FILE, EventBloom, EventBloomReader};
pub use query_bitmap::QueryBitmap;
pub use transfer_bloom::{
    ERC20_EVENTS_BLOOM_FILE, Erc20EventBloom, Erc20EventBloomReader, TRANSFER_BLOOM_FILE,
    TransferBloom, TransferBloomReader, approval_topic0, is_common_erc20_event_topic0,
    transfer_topic0,
};
