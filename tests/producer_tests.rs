#[cfg(test)]
mod args_tests {
    use clap::Parser;
    pub use TextBlaster::config::producer::Args;

    #[test]
    fn test_parse_all_args() {
        let args = Args::parse_from(&[
            "producer",
            "-i",
            "input.parquet",
            "--text-column",
            "content",
            "--id-column",
            "doc_id",
            "-a",
            "amqp://user:pass@host:port/vhost",
            "-q",
            "my_tasks",
            "-r",
            "my_results",
            "-o",
            "processed.parquet",
            "-e",
            "errors.parquet",
            "--metrics-port",
            "9090",
        ]);
        assert_eq!(args.input_file, "input.parquet");
        assert_eq!(args.text_column, "content");
        assert_eq!(args.id_column, Some("doc_id".to_string()));
        assert_eq!(args.amqp_addr, "amqp://user:pass@host:port/vhost");
        assert_eq!(args.task_queue, "my_tasks");
        assert_eq!(args.results_queue, "my_results");
        assert_eq!(args.output_file, "processed.parquet");
        assert_eq!(args.excluded_file, "errors.parquet");
        assert_eq!(args.metrics_port, Some(9090));
    }
    #[test]
    fn test_parse_required_only() {
        let args = Args::parse_from(&["producer", "-i", "input.parquet"]);
        assert_eq!(args.input_file, "input.parquet");
        assert_eq!(args.text_column, "text");
        assert_eq!(args.id_column, None);
        assert_eq!(args.amqp_addr, "amqp://guest:guest@localhost:5672/%2f");
        assert_eq!(args.task_queue, "task_queue");
        assert_eq!(args.results_queue, "results_queue");
        assert_eq!(args.output_file, "output_processed.parquet");
        assert_eq!(args.excluded_file, "excluded.parquet");
        assert_eq!(args.metrics_port, None);
    }
    #[test]
    fn test_parse_with_optional_id_column() {
        let args = Args::parse_from(&[
            "producer",
            "-i",
            "input.parquet",
            "--id-column",
            "custom_id",
        ]);
        assert_eq!(args.id_column, Some("custom_id".to_string()));
    }
    #[test]
    fn test_parse_with_optional_metrics_port() {
        let args = Args::parse_from(&["producer", "-i", "input.parquet", "--metrics-port", "8000"]);
        assert_eq!(args.metrics_port, Some(8000));
    }
    #[test]
    fn test_default_values_are_applied() {
        let args = Args::parse_from(&["producer", "--input-file", "input.parquet"]);
        assert_eq!(args.text_column, "text");
        assert_eq!(args.amqp_addr, "amqp://guest:guest@localhost:5672/%2f");
    }
    #[test]
    fn test_missing_required_arg_error() {
        let result = Args::try_parse_from(&["producer"]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }
    #[test]
    fn test_invalid_metrics_port_format() {
        let result = Args::try_parse_from(&[
            "producer",
            "-i",
            "input.parquet",
            "--metrics-port",
            "not_a_port",
        ]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            clap::error::ErrorKind::ValueValidation
        );
    }
}

#[cfg(test)]
mod publish_task_tests {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use indicatif::ProgressBar;
    use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
    use lapin::protocol::basic::AMQPProperties;
    use lapin::publisher_confirm::Confirmation;
    use lapin::types::FieldTable;
    use lapin::Result as LapinResult;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::{Arc, Mutex};
    use tempfile::NamedTempFile;
    use TextBlaster::config::producer::Args;
    use TextBlaster::data_model::TextDocument;
    use TextBlaster::error::PipelineError;
    use TextBlaster::producer_logic::*;
    use TextBlaster::utils::prometheus_metrics::{
        TASKS_PUBLISHED_TOTAL, TASK_PUBLISH_ERRORS_TOTAL,
    };

    //=============== MOCK SETUP ===============//

    /// Defines the behavior of our mock publisher channel.
    #[derive(Clone, Copy)]
    enum MockBehavior {
        /// Always return a successful ACK.
        AlwaysAck,
        /// Return a NACK on the first publish attempt.
        NackOnFirstPublish,
        /// Return a generic LapinError on publish.
        FailOnPublish,
    }

    /// A mock implementation of the TaskPublisherChannel trait.
    /// It allows us to simulate RabbitMQ behavior without a real connection.
    struct MockTaskPublisherChannel {
        /// Shared state to inspect after the test runs.
        state: Arc<Mutex<MockState>>,
    }

    struct MockState {
        /// Stores the payloads that were "published".
        published_payloads: Vec<Vec<u8>>,
        /// Controls how the mock responds to publish calls.
        behavior: MockBehavior,
    }

    impl MockTaskPublisherChannel {
        fn new(behavior: MockBehavior) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockState {
                    published_payloads: Vec::new(),
                    behavior,
                })),
            }
        }
    }

    #[async_trait]
    impl TaskPublisherChannel for MockTaskPublisherChannel {
        async fn queue_declare(
            &self,
            _name: &str,
            _options: QueueDeclareOptions,
            _arguments: FieldTable,
        ) -> LapinResult<()> {
            // Return a dummy queue. Its properties don't matter for this test.
            Ok(())
        }

        async fn basic_publish(
            &self,
            _exchange: &str,
            _routing_key: &str,
            _options: BasicPublishOptions,
            payload: &[u8],
            _properties: AMQPProperties,
        ) -> LapinResult<Confirmation> {
            let mut state = self.state.lock().unwrap();
            state.published_payloads.push(payload.to_vec());

            match state.behavior {
                MockBehavior::AlwaysAck => Ok(Confirmation::Ack(Default::default())),
                MockBehavior::NackOnFirstPublish => Ok(Confirmation::Nack(Default::default())),
                MockBehavior::FailOnPublish => {
                    // CORRECTED LINE:
                    // We must construct the full AMQPError with a code and text.
                    let amqp_error = lapin::protocol::AMQPError::new(
                        lapin::protocol::AMQPErrorKind::Hard(
                            lapin::protocol::AMQPHardError::INTERNALERROR,
                        ),
                        "mock failure".into(),
                    );
                    Err(lapin::Error::ProtocolError(amqp_error))
                }
            }
        }

        async fn confirm_select(&self, _options: ConfirmSelectOptions) -> LapinResult<()> {
            Ok(())
        }
    }

    //=============== TEST HELPER FUNCTIONS ===============//

    /// Helper to create a temporary Parquet file with a specified number of documents.
    /// Returns the temp file handle (to prevent deletion), the file path, and the original docs.
    fn create_test_parquet_file(num_records: usize) -> (NamedTempFile, Vec<TextDocument>) {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();

        let ids: Vec<String> = (0..num_records).map(|i| format!("doc_{}", i)).collect();
        let texts: Vec<String> = (0..num_records)
            .map(|i| format!("This is text for doc {}.", i))
            .collect();

        let original_docs: Vec<TextDocument> = ids
            .iter()
            .zip(texts.iter())
            .map(|(id, text)| TextDocument {
                id: id.clone(),
                source: "test".to_string(),
                content: text.clone(),
                metadata: HashMap::new(),
            })
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let id_array = StringArray::from_iter_values(ids.iter());
        let text_array = StringArray::from_iter_values(texts.iter());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(text_array)],
        )
        .unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_file, original_docs)
    }

    fn create_mock_args(input_path: String) -> Args {
        Args {
            input_file: input_path,
            text_column: "text".to_string(),
            id_column: Some("id".to_string()),
            amqp_addr: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            task_queue: "test_task_queue".to_string(),
            results_queue: "result_queue".to_string(),
            output_file: "output".to_string(),
            excluded_file: "excluded".to_string(),
            metrics_port: Some(1234),
        }
    }

    //=============== TEST CASES ===============//

    #[tokio::test]
    async fn test_publish_tasks_happy_path() {
        // ARRANGE
        let num_docs = 5;
        let (_temp_file, original_docs) = create_test_parquet_file(num_docs);
        let args = create_mock_args(_temp_file.path().to_str().unwrap().to_string());
        let mock_channel = MockTaskPublisherChannel::new(MockBehavior::AlwaysAck);
        let pb = ProgressBar::hidden();

        // Reset metrics for a clean slate
        TASKS_PUBLISHED_TOTAL.reset();

        // ACT
        let result = publish_tasks(&args, &mock_channel, &pb).await;

        // ASSERT
        assert!(result.is_ok(), "Function should succeed");
        assert_eq!(
            result.unwrap(),
            num_docs as u64,
            "Should report all documents as published"
        );

        // Check metrics
        assert_eq!(
            TASKS_PUBLISHED_TOTAL.get(),
            num_docs as f64,
            "Prometheus metric for published tasks should be correct"
        );

        // Check mock state
        let state = mock_channel.state.lock().unwrap();
        assert_eq!(
            state.published_payloads.len(),
            num_docs,
            "Exactly 5 messages should have been published"
        );

        // Verify content of a published message
        let first_payload = &state.published_payloads[0];
        let deserialized_doc: TextDocument = serde_json::from_slice(first_payload).unwrap();
        assert_eq!(
            deserialized_doc.content, original_docs[0].content,
            "The content of the published message should match the source document"
        );
    }

    #[tokio::test]
    async fn test_publish_tasks_stops_on_nack() {
        // ARRANGE
        let (_temp_file, _) = create_test_parquet_file(5);
        let args = create_mock_args(_temp_file.path().to_str().unwrap().to_string());
        let mock_channel = MockTaskPublisherChannel::new(MockBehavior::NackOnFirstPublish);
        let pb = ProgressBar::hidden();

        // Reset metrics
        TASK_PUBLISH_ERRORS_TOTAL.reset();

        // ACT
        let result = publish_tasks(&args, &mock_channel, &pb).await;

        // ASSERT
        assert!(result.is_err(), "Function should fail on NACK");
        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::QueueError(_)),
            "Error should be of type QueueError"
        );
        assert!(
            err.to_string()
                .contains("Publish confirmation failed (NACK)"),
            "Error message should indicate a NACK"
        );

        // Check metrics
        assert_eq!(
            TASK_PUBLISH_ERRORS_TOTAL.get(),
            1.0,
            "Prometheus metric for publish errors should be incremented"
        );

        // Check mock state: The message was still sent before the NACK was received.
        let state = mock_channel.state.lock().unwrap();
        assert_eq!(
            state.published_payloads.len(),
            1,
            "Only one message should have been attempted before stopping"
        );
    }

    #[tokio::test]
    async fn test_publish_tasks_propagates_lapin_error() {
        // ARRANGE
        let (_temp_file, _) = create_test_parquet_file(5);
        let args = create_mock_args(_temp_file.path().to_str().unwrap().to_string());
        let mock_channel = MockTaskPublisherChannel::new(MockBehavior::FailOnPublish);
        let pb = ProgressBar::hidden();

        // ACT
        let result = publish_tasks(&args, &mock_channel, &pb).await;

        // ASSERT
        assert!(
            result.is_err(),
            "Function should fail if basic_publish returns an error"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::QueueError(_)),
            "Error should be a wrapped QueueError"
        );
    }

    #[tokio::test]
    async fn test_publish_tasks_handles_nonexistent_input_file() {
        // ARRANGE
        let args = create_mock_args("does_not_exist".to_string());
        // The mock won't even be used, as the failure happens before publishing.
        let mock_channel = MockTaskPublisherChannel::new(MockBehavior::AlwaysAck);
        let pb = ProgressBar::hidden();

        // ACT
        // Note: The error here is synchronous, as it happens during ParquetReader setup,
        // but publish_tasks wraps it in the AppResult.
        let result = publish_tasks(&args, &mock_channel, &pb).await;

        // ASSERT
        assert!(
            result.is_err(),
            "Function should fail if input file doesn't exist"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::IoError { source: _ }),
            "{}",
            format!("Error should be of type ParquetError not {}", err)
        );
    }
}

#[cfg(test)]
mod aggregate_results_tests {
    use futures::stream;
    use indicatif::ProgressBar;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;
    use TextBlaster::config::producer::Args;
    use TextBlaster::data_model::{ProcessingOutcome, TextDocument};
    use TextBlaster::producer_logic::*;

    fn create_mock_args(output_path: String, excluded_path: String) -> Args {
        Args {
            input_file: "input".to_string(),
            text_column: "text".to_string(),
            id_column: Some("id".to_string()),
            amqp_addr: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            task_queue: "test_task_queue".to_string(),
            results_queue: "result_queue".to_string(),
            output_file: output_path,
            excluded_file: excluded_path,
            metrics_port: Some(1234),
        }
    }

    fn setup_args() -> (Args, NamedTempFile, NamedTempFile) {
        let output_file = NamedTempFile::new().unwrap();
        let excluded_file = NamedTempFile::new().unwrap();

        let args = create_mock_args(
            output_file.path().to_str().unwrap().to_string(),
            excluded_file.path().to_str().unwrap().to_string(),
        );

        (args, output_file, excluded_file)
    }

    fn sample_document(id: String) -> TextDocument {
        TextDocument {
            id,
            source: "test".to_string(),
            content: "exciting content".to_string(),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_aggregate_results_success() {
        let doc = TextDocument {
            id: "test-id".to_string(),
            content: "Test document".to_string(),
            source: "test-src".to_string(),
            metadata: HashMap::new(),
        };

        let outcome = ProcessingOutcome::Success(doc);
        let stream = stream::iter(vec![outcome]); // no mocking Delivery or Lapin

        let tmp_output = tempfile::NamedTempFile::new().unwrap();
        let tmp_excluded = tempfile::NamedTempFile::new().unwrap();

        let args = create_mock_args(
            tmp_output.path().to_str().unwrap().to_string(),
            tmp_excluded.path().to_str().unwrap().to_string(),
        );

        let pb = ProgressBar::hidden();

        let (received, success, filtered) = aggregate_results_from_stream(&args, stream, 1, &pb)
            .await
            .unwrap();

        assert_eq!(received, 1);
        assert_eq!(success, 1);
        assert_eq!(filtered, 0);
    }

    #[tokio::test]
    async fn test_aggregate_single_success() {
        let doc = sample_document("success-1".to_string());
        let outcome = ProcessingOutcome::Success(doc.clone());
        let stream = stream::iter(vec![outcome]);

        let (args, _, _) = setup_args();
        let pb = ProgressBar::hidden();

        let (received, success, filtered) = aggregate_results_from_stream(&args, stream, 1, &pb)
            .await
            .unwrap();

        assert_eq!(received, 1);
        assert_eq!(success, 1);
        assert_eq!(filtered, 0);

        // Optionally verify file content here
        let df = tokio::task::spawn_blocking(move || {
            polars::prelude::LazyFrame::scan_parquet(args.output_file.clone(), Default::default())
                .unwrap()
                .collect()
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(df.shape().0, 1);
        assert!(df
            .column("id")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap()
            .contains("success-1"));
    }

    #[tokio::test]
    async fn test_aggregate_single_filtered() {
        let doc = sample_document("filtered-1".to_string());
        let outcome = ProcessingOutcome::Filtered {
            document: doc.clone(),
            reason: "Test filter".into(),
        };
        let stream = stream::iter(vec![outcome]);

        let (args, _, _) = setup_args();
        let pb = ProgressBar::hidden();

        let (received, success, filtered) = aggregate_results_from_stream(&args, stream, 1, &pb)
            .await
            .unwrap();

        assert_eq!(received, 1);
        assert_eq!(success, 0);
        assert_eq!(filtered, 1);

        let df = tokio::task::spawn_blocking(move || {
            polars::prelude::LazyFrame::scan_parquet(args.excluded_file.clone(), Default::default())
                .unwrap()
                .collect()
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(df.shape().0, 1);
        assert!(df
            .column("id")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap()
            .contains("filtered-1"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aggregate_single_error() {
        let doc = sample_document("error-1".to_string());
        let outcome = ProcessingOutcome::Error {
            document: doc,
            error_message: "Boom".into(),
            worker_id: "w123".into(),
        };
        let stream = stream::iter(vec![outcome]);

        let (args, _, _) = setup_args();
        let pb = ProgressBar::hidden();

        let (received, success, filtered) = aggregate_results_from_stream(&args, stream, 1, &pb)
            .await
            .unwrap();

        assert_eq!(received, 1);
        assert_eq!(success, 0);
        assert_eq!(filtered, 0);

        let out_rows = tokio::task::spawn_blocking(move || {
            polars::prelude::LazyFrame::scan_parquet(&args.output_file, Default::default())
                .unwrap()
                .collect()
        })
        .await
        .unwrap()
        .unwrap()
        .height();
        let excl_rows = tokio::task::spawn_blocking(move || {
            polars::prelude::LazyFrame::scan_parquet(&args.excluded_file, Default::default())
                .unwrap()
                .collect()
        })
        .await
        .unwrap()
        .unwrap()
        .height();
        assert_eq!(out_rows, 0);
        assert_eq!(excl_rows, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aggregate_mixed_outcomes() {
        let doc1 = sample_document("d1".to_string());
        let doc2 = sample_document("d2".to_string());
        let doc3 = sample_document("d3".to_string());

        let stream = stream::iter(vec![
            ProcessingOutcome::Success(doc1.clone()),
            ProcessingOutcome::Filtered {
                document: doc2.clone(),
                reason: "Too short".into(),
            },
            ProcessingOutcome::Error {
                document: doc3,
                error_message: "Crash".into(),
                worker_id: "w1".into(),
            },
        ]);

        let (args, _, _) = setup_args();
        let pb = ProgressBar::hidden();

        let (received, success, filtered) = aggregate_results_from_stream(&args, stream, 3, &pb)
            .await
            .unwrap();

        assert_eq!(received, 3);
        assert_eq!(success, 1);
        assert_eq!(filtered, 1);

        let out_rows = tokio::task::spawn_blocking(move || {
            polars::prelude::LazyFrame::scan_parquet(&args.output_file, Default::default())
                .unwrap()
                .collect()
        })
        .await
        .unwrap()
        .unwrap()
        .height();
        let excl_rows = tokio::task::spawn_blocking(move || {
            polars::prelude::LazyFrame::scan_parquet(&args.excluded_file, Default::default())
                .unwrap()
                .collect()
        })
        .await
        .unwrap()
        .unwrap()
        .height();
        assert_eq!(out_rows, 1);
        assert_eq!(excl_rows, 1);
    }
}
