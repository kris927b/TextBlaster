// src/readers/parquet_reader.rs

use crate::config::ParquetInputConfig;
use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result}; // Use the crate's error types

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
// use std::sync::Arc; // Use Arc for shared ownership where needed by Arrow/Parquet APIs

/// Reads TextDocuments from a Parquet file.
#[derive(Debug)]
pub struct ParquetReader {
    config: ParquetInputConfig,
}

impl ParquetReader {
    /// Creates a new ParquetReader with the given configuration.
    pub fn new(config: ParquetInputConfig) -> Self {
        ParquetReader { config }
    }

    /// Reads the Parquet file and returns an iterator over TextDocuments.
    /// This consumes the reader.
    /// Note: This example reads the whole file. For very large files,
    /// you might adapt this to stream row groups.
    pub fn read_documents(self) -> Result<impl Iterator<Item = Result<TextDocument>>> {
        // Open the Parquet file
        let file = File::open(&self.config.path)?;

        // Create a Parquet reader builder
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Configure batch size if provided
        let builder = if let Some(batch_size) = self.config.batch_size {
            builder.with_batch_size(batch_size)
        } else {
            builder
        };

        // Build the reader
        let record_batch_reader = builder.build()?;

        // Get the schema to validate required columns
        let schema = record_batch_reader.schema();

        // Find the index of the text column (required)
        let text_col_idx = schema.index_of(&self.config.text_column).map_err(|_| {
            PipelineError::ConfigError(format!(
                "Text column '{}' not found in Parquet schema.",
                self.config.text_column
            ))
        })?;
        // Validate text column type (must be Utf8 or LargeUtf8)
        match schema.field(text_col_idx).data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {} // OK
            other => {
                return Err(PipelineError::ConfigError(format!(
                    "Expected text column '{}' to be Utf8 or LargeUtf8, but found {:?}",
                    self.config.text_column, other
                )));
            }
        }

        // Find the index of the ID column (optional)
        let id_col_idx: Option<usize> = match &self.config.id_column {
            Some(id_col_name) => {
                let idx = schema.index_of(id_col_name).map_err(|_| {
                    PipelineError::ConfigError(format!(
                        "ID column '{}' not found in Parquet schema.",
                        id_col_name
                    ))
                })?;
                // Optional: Validate ID column type (e.g., Utf8, Int64) - omitted for brevity
                Some(idx)
            }
            None => None,
        };

        // --- Create the Iterator ---
        // The iterator processes batches and yields individual documents.
        // Clone variables needed multiple times inside the flat_map closure *before* the closure.
        let text_column_name_outer = self.config.text_column.clone();
        let id_column_name_outer = self.config.id_column.clone();
        let config_path_outer = self.config.path.clone();

        let iterator = record_batch_reader
            .into_iter()
            .flat_map(move |batch_result| { // Process each batch. flat_map already flattens the Vec<_> produced below.
                // Use the clones from the outer scope inside this closure
                let text_column_name = text_column_name_outer.clone();
                let id_column_name = id_column_name_outer.clone();
                let config_path = config_path_outer.clone();

                match batch_result {
                    Ok(batch) => {
                        // Extract the text column array
                        let text_array = batch
                            .column(text_col_idx)
                            .as_any()
                            .downcast_ref::<StringArray>() // Assumes Utf8, adjust if LargeUtf8 needed
                            .ok_or_else(|| PipelineError::Unexpected(format!("Column '{}' is not a valid Utf8 StringArray", text_column_name)));

                        // Extract the ID column array (if configured)
                        let id_array_opt = match id_col_idx {
                            Some(idx) => {
                                // Example: Assuming ID is also a String. Adapt if it's Int64Array, etc.
                                batch.column(idx)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .ok_or_else(|| PipelineError::Unexpected(format!("ID Column '{}' is not a valid Utf8 StringArray", id_column_name.as_deref().unwrap_or("N/A"))))
                                    .map(Some) // Wrap in Ok(Some(...))
                            },
                            None => Ok(None), // No ID column configured
                        };

                        // Combine results and iterate through rows
                        match (text_array, id_array_opt) {
                             (Ok(texts), Ok(ids_opt)) => {
                                let num_rows = batch.num_rows();
                                // Clone variables needed inside the inner `map` closure *before* the closure.
                                // This ensures each row's closure gets its own copy.
                                let source_path_for_rows = config_path.clone();
                                let text_column_name_for_rows = text_column_name.clone();

                                (0..num_rows)
                                    .map(move |i| { // Process each row within the batch. This is the inner `move` closure.
                                        // Use the clones specific to this batch's row processing
                                        let source_path = source_path_for_rows.clone();
                                        let text_column_name = text_column_name_for_rows.clone();

                                        if texts.is_null(i) {
                                            // Skip rows where the essential text content is null
                                            // Return Err to propagate the issue
                                            // Now uses the cloned `text_column_name` for this specific row closure instance
                                            return Err(PipelineError::Unexpected(format!("Row {} in source '{}' has null value in text column '{}'", i, source_path, text_column_name)));
                                        }
                                        // Get text value, ensuring it's valid UTF-8 (though StringArray should guarantee this)
                                        let content = texts.value(i).to_string();

                                        // Get ID value (use row index as fallback if no ID column)
                                        let id = match (ids_opt, id_col_idx) {
                                            // Note: ids_opt is already captured correctly by the inner map closure
                                            (Some(ids), Some(_)) => {
                                                if ids.is_null(i) {
                                                    // Handle null IDs if necessary, here we default to row index string + source hint
                                                    format!("{}_row_{}", source_path, i) // Or return an error: Err(PipelineError::DataFormat(...))
                                                } else {
                                                    ids.value(i).to_string()
                                                }
                                            }
                                            _ => format!("{}_row_{}", source_path, i), // Fallback ID using source and row index
                                        };

                                        // Create the TextDocument
                                        Ok(TextDocument {
                                            id,
                                            content,
                                            source: source_path.clone(), // Store the source file path
                                            metadata: Default::default(), // Initialize empty metadata
                                            // score: None, // Initialize optional fields
                                            // sentiment: None,
                                            // entities: None,
                                            // ... other fields initialized to None or default
                                        })
                                    })
                                    .collect::<Vec<_>>() // Collect rows for this batch into Vec<Result<TextDocument>>
                            }
                            (Err(e), _) | (_, Err(e)) => vec![Err(e)], // Propagate error if column extraction failed
                        }
                    }
                    Err(e) => {
                        // Propagate error if reading the batch failed
                        vec![Err(PipelineError::Unexpected(format!("Failed to read Parquet batch from '{}': {}", config_path, e)))]
                    }
                }
            });

        Ok(iterator)
    }
}

// --- Helper function in main.rs or config.rs potentially ---
// Example of how you might load this specific config part
// fn load_parquet_config(config_path: &str) -> Result<ParquetInputConfig> {
//     // Use the `config` crate or similar to load from a larger config file
//     // For simplicity, let's pretend it's loaded directly
//     Ok(ParquetInputConfig {
//         path: "/path/to/your/data.parquet".to_string(),
//         text_column: "text_content".to_string(),
//         id_column: Some("document_id".to_string()),
//         batch_size: Some(8192),
//     })
// }
