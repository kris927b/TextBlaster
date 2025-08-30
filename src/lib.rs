#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

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

pub mod producer_logic; // Declare the new module
pub mod server;
pub mod worker_logic;

// The AmqpConnectionManager trait and its implementation have been removed from here
// as per the new strategy focusing on TaskPublisherChannel defined in producer_logic.rs.
// This should resolve the persistent "expected trait, found struct lapin::Channel" error in this file.
