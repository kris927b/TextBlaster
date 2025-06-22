use std::fs::File;
// use std::path::Path; // {{ Remove this unused import }}
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json;
use std::sync::Arc; // For serializing metadata

// Assuming your error module and Result type are defined like this
// Adjust the import path if necessary
use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::pipeline::writers::BaseWriter;

// Define the schema for TextDocument
fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        // Store metadata as a single JSON string. Nullable if metadata might be empty.
        Field::new("metadata", DataType::Utf8, true),
    ]))
}

/// Writes TextDocuments to a Parquet file.
pub struct ParquetWriter {
    #[allow(dead_code)] // Allow unused field for now, might be used later (e.g., in Drop)
    path: String,
    schema: SchemaRef,
    writer: Option<ArrowWriter<File>>, // Keep writer open for potentially writing multiple batches
}

impl ParquetWriter {
    /// Creates a new ParquetWriter and opens the file for writing.
    /// The file will be created if it doesn't exist, or truncated if it does.
    pub fn new(path: &str) -> Result<Self> {
        let schema = create_schema();
        let file = File::create(path)?;
        let props = WriterProperties::builder()
            // Add any specific writer properties here, e.g., compression
            // .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        Ok(ParquetWriter {
            path: path.to_string(),
            schema,
            writer: Some(writer),
        })
    }
}

impl BaseWriter for ParquetWriter {
    /// Writes a batch of TextDocuments to the Parquet file.
    fn write_batch(&mut self, documents: &[TextDocument]) -> Result<()> {
        if documents.is_empty() {
            return Ok(()); // Nothing to write
        }

        if self.writer.is_none() {
            // Handle error: writer was already closed or failed to initialize
            // You might want a more specific error type here
            return Err(PipelineError::Unexpected(
                "The parquet writer was already closed".to_string(),
            ));
        }

        // 1. Convert TextDocuments to Arrow Arrays
        // Use Vec::with_capacity for slight performance improvement
        let mut ids = Vec::with_capacity(documents.len());
        let mut sources = Vec::with_capacity(documents.len());
        let mut contents = Vec::with_capacity(documents.len());
        let mut metadata_json = Vec::with_capacity(documents.len());

        for doc in documents {
            ids.push(doc.id.clone());
            sources.push(doc.source.clone());
            contents.push(doc.content.clone());
            // Serialize metadata HashMap to JSON string for each document
            if doc.metadata.is_empty() {
                metadata_json.push(None); // Represent empty metadata as null in Parquet
            } else {
                // Push Some(json_string) or None if serialization fails
                metadata_json.push(serde_json::to_string(&doc.metadata).ok());
            }
        }

        // Create Arrow Arrays
        let id_array = Arc::new(StringArray::from(ids)) as ArrayRef;
        let source_array = Arc::new(StringArray::from(sources)) as ArrayRef;
        let content_array = Arc::new(StringArray::from(contents)) as ArrayRef;
        let metadata_array = Arc::new(StringArray::from(metadata_json)) as ArrayRef; // Handles Vec<Option<String>>

        // 2. Create RecordBatch
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![id_array, source_array, content_array, metadata_array],
        )?; // Propagates ArrowError if creation fails

        // 3. Write the batch using the stored writer
        if let Some(writer) = self.writer.as_mut() {
            writer.write(&batch)?; // Propagates ParquetError
        }
        // The error case where writer is None is handled above

        Ok(())
    }

    /// Closes the Parquet writer and finalizes the file.
    /// This must be called to ensure all data is flushed and the file is valid.
    fn close(mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?; // Propagates ParquetError
        }
        Ok(())
    }
}
