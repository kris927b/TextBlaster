use crate::{
    config::parquet::ParquetInputConfig,
    data_model::TextDocument,
    error::{PipelineError, Result},
    pipeline::readers::base_reader::BaseReader,
};

use arrow::array::{
    Array, ArrayRef, Date32Array, RecordBatchReader, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{collections::HashMap, fs::File};
use tracing::warn;

pub struct ParquetReader {
    config: ParquetInputConfig,
}

impl ParquetReader {
    pub fn new(config: ParquetInputConfig) -> Self {
        Self { config }
    }

    fn get_required_column_index(schema: &SchemaRef, name: &str) -> Result<usize> {
        schema.index_of(name).map_err(|_| {
            PipelineError::ConfigError(format!("Required column '{}' not found in schema.", name))
        })
    }

    fn validate_string_column(schema: &SchemaRef, idx: usize, name: &str) -> Result<()> {
        match schema.field(idx).data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(()),
            other => Err(PipelineError::ConfigError(format!(
                "Column '{}' must be Utf8 or LargeUtf8, found: {:?}",
                name, other,
            ))),
        }
    }

    fn parse_date_or_datetime(arr: &ArrayRef, index: usize) -> Option<NaiveDateTime> {
        if let Some(dates) = arr.as_any().downcast_ref::<Date32Array>() {
            if dates.is_null(index) {
                None
            } else {
                Some(
                    NaiveDate::from_num_days_from_ce_opt(dates.value(index))?
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                )
            }
        } else if let Some(timestamps) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            if timestamps.is_null(index) {
                None
            } else {
                Some(DateTime::from_timestamp_micros(timestamps.value(index))?.naive_local())
            }
        } else {
            None
        }
    }
}

impl BaseReader for ParquetReader {
    fn read_documents(&self) -> Result<Box<dyn Iterator<Item = Result<TextDocument>>>> {
        let file = File::open(self.config.path.clone())?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        if let Some(batch_size) = self.config.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let reader = builder.build()?;
        let schema = reader.schema();

        // Required column
        let text_idx = Self::get_required_column_index(&schema, &self.config.text_column)?;
        let id_idx = Self::get_required_column_index(&schema, &self.config.id_column)?;
        Self::validate_string_column(&schema, text_idx, &self.config.text_column)?;

        // Optional column indices
        // let id_idx = self
        //     .config
        //     .id_column
        //     .as_ref()
        //     .and_then(|id| schema.index_of(id).ok());
        let source_idx = schema.index_of("source").ok();
        let added_idx = schema.index_of("added").ok();
        let created_idx = schema.index_of("created").ok();

        let metadata_idx = schema.index_of("metadata").ok().filter(|&idx| {
            matches!(
                schema.field(idx).data_type(),
                DataType::Utf8 | DataType::LargeUtf8
            )
        });

        let text_col_name = self.config.text_column.clone();
        let id_col_name = self.config.id_column.clone();
        let path_str = self.config.path.clone();

        let iter = reader
            .into_iter()
            .flat_map(move |batch_result| match batch_result {
                Ok(batch) => {
                    let num_rows = batch.num_rows();
                    let text_arr = batch
                        .column(text_idx)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Text column downcast failed");

                    let id_arr = batch
                        .column(id_idx)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Text column downcast failed");

                    // let id_arr = id_idx.and_then(|idx| {
                    //     let arr = batch.column(idx);
                    //     if let Some(arr) = arr.as_any().downcast_ref::<StringArray>() {
                    //         Some(Arc::new(arr.clone()))
                    //     } else if let Some(large) = arr.as_any().downcast_ref::<LargeStringArray>()
                    //     {
                    //         let converted: StringArray =
                    //             large.iter().map(|s| s.map(|v| v.to_string())).collect();
                    //         Some(Arc::new(converted))
                    //     } else {
                    //         None
                    //     }
                    // });

                    let source_arr = source_idx.and_then(|idx| {
                        batch
                            .column(idx)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .cloned()
                    });

                    let added_arr = added_idx.map(|idx| batch.column(idx).clone());
                    let created_arr = created_idx.map(|idx| batch.column(idx).clone());

                    let metadata_arr = metadata_idx.and_then(|idx| {
                        batch
                            .column(idx)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .cloned()
                    });

                    (0..num_rows)
                        .map({
                            let txt_value = text_col_name.clone();
                            let id_value = id_col_name.clone();
                            let path_value = path_str.clone();
                            move |i| {
                                if text_arr.is_null(i) {
                                    return Err(PipelineError::Unexpected(format!(
                                        "Row {} has null text column '{}'",
                                        i,
                                        txt_value.clone()
                                    )));
                                }

                                if id_arr.is_null(i) {
                                    return Err(PipelineError::Unexpected(format!(
                                        "Row {} has null id column '{}'",
                                        i,
                                        id_value.clone()
                                    )));
                                }

                                let id = id_arr.value(i).to_string();

                                let content = text_arr.value(i).to_string();

                                let source_val = source_arr
                                    .as_ref()
                                    .and_then(|arr| {
                                        if arr.is_null(i) {
                                            None
                                        } else {
                                            Some(arr.value(i).to_string())
                                        }
                                    })
                                    .unwrap_or_else(|| path_value.clone());

                                let added_val = added_arr
                                    .as_ref()
                                    .and_then(|arr| Self::parse_date_or_datetime(arr, i))
                                    .map(|dt| dt.date());

                                let created_val = created_arr.as_ref().and_then(|arr| {
                                    if let Some(struct_arr) =
                                        arr.as_any().downcast_ref::<StructArray>()
                                    {
                                        let field_0 = struct_arr.column(0);
                                        let field_1 = struct_arr.column(1);
                                        let left = Self::parse_date_or_datetime(field_0, i);
                                        let right = Self::parse_date_or_datetime(field_1, i);
                                        match (left, right) {
                                            (Some(a), Some(b)) => Some((a, b)),
                                            _ => None,
                                        }
                                    } else {
                                        warn!("'created' column is not a StructArray.");
                                        None
                                    }
                                });

                                let metadata = metadata_arr
                                    .as_ref()
                                    .and_then(|arr| {
                                        if arr.is_null(i) {
                                            None
                                        } else {
                                            Some(arr.value(i))
                                        }
                                    })
                                    .map(|json| {
                                        serde_json::from_str(json).unwrap_or_else(|e| {
                                            warn!(%id, %json, %e, "Failed to parse metadata JSON.");
                                            HashMap::new()
                                        })
                                    })
                                    .unwrap_or_default();

                                Ok(TextDocument {
                                    id,
                                    content,
                                    source: source_val,
                                    added: added_val,
                                    created: created_val,
                                    metadata,
                                })
                            }
                        })
                        .collect::<Vec<_>>()
                }
                Err(e) => vec![Err(PipelineError::Unexpected(format!(
                    "Failed to read batch: {}",
                    e
                )))],
            });

        Ok(Box::new(iter))
    }
}
