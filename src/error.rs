// Example using thiserror
use crate::data_model::TextDocument;
use thiserror::Error; // {{Add import for TextDocument}}

/// Custom Result type for this crate.
pub type Result<T> = std::result::Result<T, PipelineError>;

/// The Error type for pipeline operations.
#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("I/O error: {source}")]
    IoError {
        #[from]
        source: std::io::Error,
    },

    #[error("Parquet reading error: {source}")]
    ParquetError {
        #[from]
        source: parquet::errors::ParquetError,
    },

    #[error("Arrow conversion error: {source}")]
    ArrowError {
        #[from]
        source: arrow::error::ArrowError,
    },

    // {{MODIFIED: Changed 'doc_id: String' to 'document: TextDocument' and updated message}}
    #[error("Document '{document_id}' filtered out: {reason}", document_id = document.id)]
    DocumentFiltered {
        document: TextDocument,
        reason: String,
    },

    #[error("Error in processing step '{step_name}': {source}")]
    StepError {
        step_name: String,
        source: Box<PipelineError>, // Or Box<dyn std::error::Error + Send + Sync + 'static> if steps can return other errors
    },

    // {{Add a variant for Queue/Messaging errors}}
    #[error("Queueing system error: {0}")]
    QueueError(String),

    #[error("Serialization/Deserialization error: {source}")]
    SerializationError {
        #[from]
        source: serde_json::Error, // Specific to JSON for now, could generalize
    },

    #[error("Unexpected error: {0}")]
    Unexpected(String),
    // Add other specific error types as needed
}

// We could implement From<lapin::Error> here, but since lapin::Error doesn't
// directly implement std::error::Error sometimes, mapping it where it occurs
// and converting to a String might be more straightforward for now.
impl From<lapin::Error> for PipelineError {
    fn from(err: lapin::Error) -> Self {
        PipelineError::QueueError(err.to_string())
    }
}
