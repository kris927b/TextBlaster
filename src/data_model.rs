use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TextDocument {
    pub id: String,
    pub content: String,
    pub source: String,
    pub added: Option<NaiveDate>, // or NaiveDateTime, depending
    pub created: Option<(NaiveDateTime, NaiveDateTime)>, // updated
    pub metadata: HashMap<String, String>,
}

// TODO: Add more fields as they are discovered

/// Represents the outcome of processing a single document task.
/// This is sent from the worker back to the producer via the results queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessingOutcome {
    /// The document was successfully processed through all pipeline steps.
    Success(TextDocument),
    /// The document was filtered out by one of the pipeline steps.
    Filtered {
        document: TextDocument,
        reason: String,
    },
    /// An error occurred during processing a pipeline step.
    Error {
        document: TextDocument,
        error_message: String,
        worker_id: String,
    },
}
