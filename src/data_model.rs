use std::collections::HashMap;
use serde::{Serialize, Deserialize};

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
