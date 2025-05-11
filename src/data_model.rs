use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)] // Clone might be needed depending on ownership strategy
pub struct TextDocument {
    pub id: String,     // Unique identifier (e.g., filename, database ID)
    pub source: String, // Original source location/identifier
    pub content: String,
    pub metadata: HashMap<String, String>, // For intermediate results or context
                                           // pub score: Option<f64>, // Example field added by a scoring step
                                           // Add other fields as needed by your steps
}

// TODO: Add more fields as they are discovered

/// Represents the outcome of processing a single document task.
/// This is sent from the worker back to the producer via the results queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessingOutcome {
    /// The document was successfully processed through all pipeline steps.
    Success(TextDocument),
    /// The document was filtered out by one of the pipeline steps.
    Filtered { document: TextDocument, reason: String },
    // Potential future extension:
    // Error { id: String, error_details: String },
}
