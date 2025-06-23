// src/pipeline/readers/mod.rs

pub mod base_reader;
pub mod parquet_reader; // Looks for src/pipeline/readers/parquet_reader.rs
                        // Often good practice to re-export the main types
pub use base_reader::BaseReader;
pub use parquet_reader::ParquetReader; // Assuming ParquetReader struct/enum exists
