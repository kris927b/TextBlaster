// src/readers/parquet_reader.rs

use crate::config::ParquetInputConfig;
use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result}; // Use the crate's error types

use arrow::array::{Array, LargeStringArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatchReader; // Renamed from `arrow::record_batch::RecordBatch` which is a struct
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File; // Needed for metadata
use std::sync::Arc;
use tracing::{debug, warn}; // Use Arc for shared ownership where needed by Arrow/Parquet APIs // Added warn

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
    pub fn read_documents(self) -> Result<impl Iterator<Item = Result<TextDocument>>> {
        let file = File::open(&self.config.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let builder = if let Some(batch_size) = self.config.batch_size {
            builder.with_batch_size(batch_size)
        } else {
            builder
        };
        let record_batch_reader = builder.build()?;
        let schema = record_batch_reader.schema();

        let text_col_idx = schema.index_of(&self.config.text_column).map_err(|_| {
            PipelineError::ConfigError(format!(
                "Text column '{}' not found in Parquet schema.",
                self.config.text_column
            ))
        })?;
        match schema.field(text_col_idx).data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {}
            other => {
                return Err(PipelineError::ConfigError(format!(
                    "Expected text column '{}' to be Utf8 or LargeUtf8, but found {:?}",
                    self.config.text_column, other
                )));
            }
        }

        let id_col_idx: Option<usize> = match &self.config.id_column {
            Some(id_col_name) => {
                let idx = schema.index_of(id_col_name).map_err(|_| {
                    PipelineError::ConfigError(format!(
                        "ID column '{}' not found in Parquet schema.",
                        id_col_name
                    ))
                })?;
                Some(idx)
            }
            None => None,
        };

        // {{ Add logic to find metadata column }}
        // Assume the metadata column is named "metadata" by convention from ParquetWriter
        let metadata_col_name = "metadata"; // Or make this configurable via ParquetInputConfig
        let metadata_col_idx: Option<usize> = match schema.index_of(metadata_col_name) {
            Ok(idx) => {
                // Optional: Validate metadata column type (should be Utf8/LargeUtf8 for JSON string)
                match schema.field(idx).data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 => Some(idx),
                    _ => {
                        // Log a warning or return an error if type is not string-like
                        // For now, let's ignore if type is wrong, effectively skipping metadata.
                        // Or, more strictly:
                        // return Err(PipelineError::ConfigError(format!(
                        //     "Metadata column '{}' is not Utf8 or LargeUtf8.", metadata_col_name
                        // )));
                        warn!(column_name = %metadata_col_name, "Metadata column found but is not a string type. Metadata will not be loaded.");
                        None
                    }
                }
            }
            Err(_) => {
                // Metadata column not found, which is acceptable.
                None
            }
        };

        let text_column_name_outer = self.config.text_column.clone();
        let id_column_name_outer = self.config.id_column.clone();
        let config_path_outer = self.config.path.clone();

        let iterator = record_batch_reader
            .into_iter()
            .flat_map(move |batch_result| {
                let text_column_name = text_column_name_outer.clone();
                let id_column_name = id_column_name_outer.clone();
                let config_path = config_path_outer.clone();

                match batch_result {
                    Ok(batch) => {
                        let text_array_res = batch
                            .column(text_col_idx)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| PipelineError::Unexpected(format!("Column '{}' is not a valid Utf8 StringArray", text_column_name)));

                        let id_array_opt_res: Result<Option<Arc<StringArray>>> = match id_col_idx {
                            Some(idx) => {
                                let column = batch.column(idx);
                                debug!("ParquetReader: ID column data type in batch: {:?}", column.data_type());
                                if let Some(ids_arr) = column.as_any().downcast_ref::<StringArray>() {
                                    debug!("ParquetReader: Successfully downcasted ID column to StringArray.");
                                    Ok(Some(Arc::new(ids_arr.clone()))) // Clone the StringArray and wrap in Arc
                                } else if let Some(ids_arr) = column.as_any().downcast_ref::<LargeStringArray>() {
                                    debug!("ParquetReader: ID column is LargeUtf8. Attempting to convert to StringArray.");
                                    let string_array: StringArray = ids_arr.iter().map(|s| s.map(|s_val| s_val.to_string())).collect();
                                    Ok(Some(Arc::new(string_array))) // Wrap the new StringArray in Arc
                                }
                                else {
                                    Err(PipelineError::Unexpected(format!("ID Column '{}' is not a valid Utf8 or LargeUtf8 StringArray, found {:?}", id_column_name.as_deref().unwrap_or("N/A"), column.data_type())))
                                }
                            },
                            None => Ok(None),
                        };

                        // {{ Add logic to extract metadata array }}
                        let metadata_array_opt_res = match metadata_col_idx {
                            Some(idx) => batch.column(idx)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .ok_or_else(|| PipelineError::Unexpected(format!("Metadata Column '{}' is not a valid Utf8 StringArray", metadata_col_name))) // metadata_col_name is captured
                                .map(Some),
                            None => Ok(None),
                        };

                        match (text_array_res, id_array_opt_res, metadata_array_opt_res) {
                             (Ok(texts), Ok(ids_opt_arc), Ok(metadata_opt)) => {
                                let num_rows = batch.num_rows();
                                let source_path_for_rows = config_path.clone();

                                (0..num_rows)
                                    .map(move |i| {
                                        let source_path = source_path_for_rows.clone(); // Cloned for this row

                                        if texts.is_null(i) {
                                            return Err(PipelineError::Unexpected(format!("Row {} in source '{}' has null value in text column '{}'", i, source_path, text_column_name))); // text_column_name is captured
                                        }
                                        let content = texts.value(i).to_string();

                                        debug!("ParquetReader: ids_opt_arc before ID assignment: {:#?}", ids_opt_arc);

                                        let id = match &ids_opt_arc { // ids_opt_arc is Option<Arc<StringArray>>
                                            Some(ids_arr_arc) => { // ids_arr_arc is Arc<StringArray>
                                                let ids_arr = ids_arr_arc.as_ref(); // Get &StringArray from Arc
                                                if ids_arr.is_null(i) {
                                                    debug!("ParquetReader: ID is null for row {:?}. Generating ID.", ids_arr);
                                                    format!("{}_row_{}", source_path, i)
                                                } else {
                                                    ids_arr.value(i).to_string()
                                                }
                                            }
                                            _ => format!("{}_row_{}", source_path, i),
                                        };

                                        // {{ Add logic to parse metadata string }}
                                        let metadata_map: HashMap<String, String> = match metadata_opt { // metadata_opt is captured
                                            Some(metadata_arr) if !metadata_arr.is_null(i) => {
                                                let json_str = metadata_arr.value(i);
                                                if json_str.is_empty() {
                                                    HashMap::new()
                                                } else {
                                                    serde_json::from_str(json_str).unwrap_or_else(|e| {
                                                        warn!(doc_id = %id, json = %json_str, error = %e, "Failed to parse metadata JSON. Using empty metadata.");
                                                        HashMap::new()
                                                    })
                                                }
                                            }
                                            _ => HashMap::new(), // No metadata column, or value is null
                                        };

                                        Ok(TextDocument {
                                            id,
                                            content,
                                            source: source_path.clone(),
                                            metadata: metadata_map, // Use parsed metadata
                                        })
                                    })
                                    .collect::<Vec<_>>()
                            }
                            // Handle errors from array downcasting/extraction
                            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => vec![Err(e)],
                        }
                    }
                    Err(e) => {
                        vec![Err(PipelineError::Unexpected(format!("Failed to read Parquet batch from '{}': {}", config_path, e)))]
                    }
                }
            });

        Ok(iterator)
    }
}
