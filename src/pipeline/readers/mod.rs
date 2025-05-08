// src/pipeline/readers/mod.rs

pub mod parquet_reader; // Looks for src/pipeline/readers/parquet_reader.rs

// Often good practice to re-export the main types
pub use parquet_reader::ParquetReader; // Assuming ParquetReader struct/enum exists
