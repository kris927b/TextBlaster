// Declare the modules that form the library's public API (or internal structure)
// Using `pub mod` makes them accessible from the binaries using `use rust_data::module_name;`
pub mod config;
pub mod data_model;
pub mod error;
pub mod executor;
pub mod pipeline;
pub mod utils;

// You might also want to re-export common types for convenience, e.g.:
// pub use error::{Result, PipelineError};
// pub use data_model::TextDocument;
// pub use executor::{PipelineExecutor, ProcessingStep};
