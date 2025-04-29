// src/pipeline/mod.rs

pub mod filters;
pub mod readers; // Looks for src/pipeline/readers.rs OR src/pipeline/readers/mod.rs // Looks for src/pipeline/filters.rs OR src/pipeline/filters/mod.rs

// You could also define things common to the whole pipeline here
// pub trait ProcessingStep { ... }
