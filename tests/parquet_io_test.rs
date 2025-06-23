use chrono::{NaiveDate, NaiveDateTime};
use std::collections::HashMap;
use tempfile::NamedTempFile;
use TextBlaster::config::parquet::ParquetInputConfig;
use TextBlaster::data_model::TextDocument;
use TextBlaster::error::Result;
use TextBlaster::pipeline::readers::{BaseReader, ParquetReader};
use TextBlaster::pipeline::writers::{BaseWriter, ParquetWriter};

fn create_sample_doc(
    id: &str,
    content: &str,
    source: &str,
    meta: Option<HashMap<String, String>>,
    added: Option<NaiveDate>,
    created: Option<(NaiveDateTime, NaiveDateTime)>,
) -> TextDocument {
    TextDocument {
        id: id.to_string(),
        content: content.to_string(),
        source: source.to_string(),
        metadata: meta.unwrap_or_default(),
        added,
        created,
    }
}

#[test]
fn test_parquet_read_write_roundtrip() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let file_path_str = temp_file.path().to_str().unwrap();

    let added = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let created_start =
        NaiveDateTime::parse_from_str("2024-01-01T12:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let created_end =
        NaiveDateTime::parse_from_str("2024-01-02T12:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();

    let mut original_docs = vec![
        create_sample_doc(
            "doc1",
            "Hello Parquet!",
            "file1.txt",
            Some(
                [("key1".to_string(), "val1".to_string())]
                    .into_iter()
                    .collect(),
            ),
            Some(added),
            Some((created_start, created_end)),
        ),
        create_sample_doc("doc2", "Test doc 2", "file2.txt", None, None, None),
        create_sample_doc(
            "doc3",
            "Final doc",
            "file3.txt",
            Some(
                [("lang".to_string(), "en".to_string())]
                    .into_iter()
                    .collect(),
            ),
            Some(added),
            Some((created_start, created_end)),
        ),
    ];

    let mut writer = ParquetWriter::new(file_path_str)?;
    writer.write_batch(&original_docs)?;
    writer.close()?;

    let reader_config = ParquetInputConfig {
        path: file_path_str.to_string(),
        text_column: "text".to_string(),
        id_column: "id".to_string(),
        batch_size: Some(10),
    };

    let reader = ParquetReader::new(reader_config);
    let mut read_docs = vec![];
    for result in reader.read_documents()? {
        read_docs.push(result?);
    }

    assert_eq!(original_docs.len(), read_docs.len());

    original_docs.sort_by(|a, b| a.id.cmp(&b.id));
    read_docs.sort_by(|a, b| a.id.cmp(&b.id));

    for (original, read) in original_docs.iter().zip(read_docs.iter()) {
        assert_eq!(original.id, read.id);
        assert_eq!(original.content, read.content);
        assert_eq!(original.source, read.source);
        assert_eq!(original.metadata, read.metadata);
        assert_eq!(original.added, read.added);
        assert_eq!(original.created, read.created);
    }

    Ok(())
}

#[test]
fn test_parquet_missing_column_handling() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let file_path_str = temp_file.path().to_str().unwrap();

    // Write only ID and text
    let docs = vec![TextDocument {
        id: "missing".into(),
        content: "missing column".into(),
        source: "dummy".into(),
        metadata: HashMap::new(),
        added: None,
        created: None,
    }];

    let mut writer = ParquetWriter::new(file_path_str)?;
    writer.write_batch(&docs)?;
    writer.close()?;

    // Now try to read it with a config expecting a column that doesn't exist
    let bad_config = ParquetInputConfig {
        path: file_path_str.to_string(),
        text_column: "text".to_string(),
        id_column: "nonexistent_id".to_string(), // This column does not exist
        batch_size: None,
    };

    let reader = ParquetReader::new(bad_config);
    let result = reader.read_documents();
    assert!(result.is_err(), "Expected error due to missing column");

    Ok(())
}

#[test]
fn test_parquet_file_readable_in_duckdb() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let file_path_str = temp_file.path().to_str().unwrap();

    let docs = vec![create_sample_doc("a", "text", "source", None, None, None)];

    let mut writer = ParquetWriter::new(file_path_str)?;
    writer.write_batch(&docs)?;
    writer.close()?;

    let conn = duckdb::Connection::open_in_memory().expect("DuckDB in-memory open failed");
    let query = format!(
        "SELECT * FROM read_parquet('{}')",
        file_path_str.replace("'", "''")
    );
    let mut stmt = conn.prepare(&query).expect("DuckDB query failed");
    let rows = stmt
        .query_map([], |row| {
            let id: String = row.get(0)?;
            Ok(id)
        })
        .expect("Query mapping failed in parquet test");

    let mut found_row = false;
    for id_result in rows {
        let id = id_result.expect("Reading id from row failed in parquet test");
        assert_eq!(id, "a");
        found_row = true;
    }
    assert!(found_row, "DuckDB did not return any rows");
    Ok(())
}
