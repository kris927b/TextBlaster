use std::collections::HashMap;
use tempfile::NamedTempFile;

// Import necessary items from your crate
use TextBlaster::config::ParquetInputConfig; // Assuming this is public and in src/config.rs
use TextBlaster::data_model::TextDocument;
use TextBlaster::error::Result;
use TextBlaster::pipeline::readers::parquet_reader::ParquetReader;
use TextBlaster::pipeline::writers::parquet_writer::ParquetWriter; // Assuming a top-level Result or specific error type from src/error.rs

// Helper function to create TextDocuments easily for tests
fn create_sample_doc(
    id: &str,
    content: &str,
    source: &str,
    meta: Option<HashMap<String, String>>,
) -> TextDocument {
    TextDocument {
        id: id.to_string(),
        content: content.to_string(),
        source: source.to_string(),
        metadata: meta.unwrap_or_default(),
    }
}

#[test]
fn test_parquet_read_write_roundtrip() -> Result<()> {
    // 1. Create a temporary file for writing Parquet data
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let file_path_str = temp_file
        .path()
        .to_str()
        .expect("Temp file path is not valid UTF-8");

    // 2. Define sample documents
    let mut original_docs = vec![
        create_sample_doc(
            "doc1",
            "Hello Parquet!",
            "source1",
            Some(
                [("key1".to_string(), "val1".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
            ),
        ),
        create_sample_doc("doc2", "Testing read and write.", "source2", None),
        create_sample_doc(
            "doc3",
            "Another document with metadata.",
            "source3",
            Some(
                [
                    ("lang".to_string(), "en".to_string()),
                    ("topic".to_string(), "test".to_string()),
                ]
                .iter()
                .cloned()
                .collect(),
            ),
        ),
    ];

    // 3. Write documents using ParquetWriter
    let mut writer = ParquetWriter::new(file_path_str)?;
    writer.write_batch(&original_docs)?;
    writer.close()?; // Important to close the writer to finalize the file

    // 4. Read documents back using ParquetReader
    // Ensure ParquetInputConfig is accessible via rust_data::config::ParquetInputConfig
    // and its fields are public.
    let reader_config = ParquetInputConfig {
        path: file_path_str.to_string(),
        text_column: "content".to_string(), // Matches the field name in TextDocument and schema in ParquetWriter
        id_column: Some("id".to_string()),  // Matches the field name
        batch_size: Some(10),               // Optional, can be tested
    };
    let reader = ParquetReader::new(reader_config);

    // Collect results, handling potential errors during read for each document
    let mut read_docs: Vec<TextDocument> = Vec::new();
    let document_iterator = reader.read_documents()?;
    for result in document_iterator {
        match result {
            Ok(doc) => read_docs.push(doc),
            Err(e) => {
                // If a single document fails to parse, we might want to log it or fail the test.
                // For this test, let's propagate the error.
                eprintln!("Error reading document: {:?}", e);
                return Err(e);
            }
        }
    }

    // 5. Assertions
    assert_eq!(
        original_docs.len(),
        read_docs.len(),
        "Number of documents should match"
    );

    // Sort both lists by ID to ensure consistent order for comparison
    // This is important because Parquet doesn't guarantee row order.
    original_docs.sort_by(|a, b| a.id.cmp(&b.id));
    read_docs.sort_by(|a, b| a.id.cmp(&b.id));

    for (original, read) in original_docs.iter().zip(read_docs.iter()) {
        assert_eq!(
            original.id, read.id,
            "Document ID should match for ID: {}",
            original.id
        );
        assert_eq!(
            original.content, read.content,
            "Document content should match for ID: {}",
            original.id
        );

        // ParquetReader currently sets 'source' to the file path.
        assert_eq!(
            read.source, file_path_str,
            "Read document source should be the file path for ID: {}",
            original.id
        );

        // {{ Updated metadata assertion }}
        // Now that ParquetReader attempts to read metadata, compare it with the original.
        // Note: If original metadata was None (passed to create_sample_doc), it becomes an empty HashMap.
        // The reader will also produce an empty HashMap if metadata column is missing, null, or JSON is empty/invalid.
        assert_eq!(
            original.metadata, read.metadata,
            "Document metadata should match for ID: {}",
            original.id
        );
    }

    Ok(())
}
