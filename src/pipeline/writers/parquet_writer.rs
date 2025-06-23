use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Builder, RecordBatch, StringBuilder, StructArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chrono::Datelike;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json;

use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::pipeline::writers::BaseWriter;

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("added", DataType::Date32, true),
        Field::new(
            "created",
            DataType::Struct(
                vec![
                    Field::new(
                        "start",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    Field::new(
                        "end",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        ),
        Field::new("metadata", DataType::Utf8, true),
    ]))
}

use arrow::datatypes::TimeUnit;

/// Writes TextDocuments to a Parquet file.
pub struct ParquetWriter {
    schema: SchemaRef,
    writer: Option<ArrowWriter<File>>,
}

impl ParquetWriter {
    pub fn new(path: &str) -> Result<Self> {
        let schema = create_schema();
        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        Ok(ParquetWriter {
            schema,
            writer: Some(writer),
        })
    }
}

impl BaseWriter for ParquetWriter {
    fn write_batch(&mut self, documents: &[TextDocument]) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        let mut id_builder = StringBuilder::new(); // documents.len());
        let mut source_builder = StringBuilder::new(); // documents.len());
        let mut text_builder = StringBuilder::new(); // documents.len());
        let mut added_builder = Date32Builder::new(); // documents.len());
        let mut created_start_builder = TimestampMicrosecondBuilder::new(); // documents.len());
        let mut created_end_builder = TimestampMicrosecondBuilder::new(); // documents.len());
        let mut metadata_builder = StringBuilder::new(); //documents.len());

        for doc in documents {
            id_builder.append_value(&doc.id);
            source_builder.append_value(&doc.source);
            text_builder.append_value(&doc.content);

            // added: Option<NaiveDate>
            if let Some(date) = doc.added {
                let days = date.num_days_from_ce();
                added_builder.append_value(days);
            } else {
                added_builder.append_null();
            }

            // created: Option<(NaiveDateTime, NaiveDateTime)>
            if let Some((start, end)) = &doc.created {
                created_start_builder.append_value(start.and_utc().timestamp_micros());
                created_end_builder.append_value(end.and_utc().timestamp_micros());
            } else {
                created_start_builder.append_null();
                created_end_builder.append_null();
            }

            if doc.metadata.is_empty() {
                metadata_builder.append_null();
            } else {
                let json = serde_json::to_string(&doc.metadata).map_err(|e| {
                    PipelineError::Unexpected(format!("Metadata serialization failed: {e}"))
                })?;
                metadata_builder.append_value(json);
            }
        }

        let id_array = Arc::new(id_builder.finish()) as ArrayRef;
        let source_array = Arc::new(source_builder.finish()) as ArrayRef;
        let text_array = Arc::new(text_builder.finish()) as ArrayRef;
        let added_array = Arc::new(added_builder.finish()) as ArrayRef;

        let created_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "start",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(created_start_builder.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "end",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )),
                Arc::new(created_end_builder.finish()) as ArrayRef,
            ),
        ]));

        let metadata_array = Arc::new(metadata_builder.finish()) as ArrayRef;

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                id_array,
                source_array,
                text_array,
                added_array,
                created_array,
                metadata_array,
            ],
        )?;

        if let Some(writer) = self.writer.as_mut() {
            writer.write(&batch)?;
        }

        Ok(())
    }

    fn close(mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(())
    }
}
