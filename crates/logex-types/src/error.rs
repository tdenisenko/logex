use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogExError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("index error: {0}")]
    Index(String),

    #[error("query parse error: {0}")]
    QueryParse(String),

    #[error("query execution error: {0}")]
    QueryExecution(String),

    #[error("ingestion error: {0}")]
    Ingestion(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}
