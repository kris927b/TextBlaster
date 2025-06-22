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
mod publish_tasks_tests {
    use std::collections::HashMap;

    use indicatif::ProgressBar;
    use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
    use tempfile::NamedTempFile;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage,
    };
    use tokio::time::{sleep, Duration, Instant};
    use uuid::Uuid;
    // Added AsyncRunner
    use TextBlaster::config::producer::Args;
    use TextBlaster::data_model::TextDocument;
    use TextBlaster::error::Result;
    use TextBlaster::pipeline::writers::{BaseWriter, ParquetWriter};
    use TextBlaster::producer_logic::publish_tasks; // replace `my_crate` with your actual crate

    fn create_mock_args(input_path: String, queue_name: String) -> Args {
        Args {
            input_file: input_path,
            text_column: "text".to_string(),
            id_column: Some("id".to_string()),
            amqp_addr: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            task_queue: queue_name,
            results_queue: "result_queue".to_string(),
            prefetch_count: 10,
            output_file: "output".to_string(),
            excluded_file: "excluded".to_string(),
            metrics_port: Some(1234),
        }
    }

    // Helper function to start a RabbitMQ container
    async fn start_rabbitmq_container() -> (ContainerAsync<GenericImage>, Channel, String) {
        let image = GenericImage::new("rabbitmq", "3.13-management")
            .with_wait_for(WaitFor::message_on_stdout(
                "Server startup complete".to_string(),
            ))
            .with_exposed_port(5672.tcp()); // Default AMQP port

        // Use AsyncRunner for async test environments
        let container = image
            .start()
            .await
            .expect("Failed to start RabbitMQ container");

        let host_ip = container
            .get_host()
            .await
            .expect("Failed to get container host IP");
        let host_port = container
            .get_host_port_ipv4(5672)
            .await
            .expect("Failed to get mapped port");

        let amqp_addr = format!("amqp://guest:guest@{}:{}/%2f", host_ip, host_port);

        let conn = Connection::connect(&amqp_addr, ConnectionProperties::default())
            .await
            .expect("connection failed");
        let channel = conn.create_channel().await.expect("channel failed");

        let queue_name = format!("test_q_{}", Uuid::new_v4());

        channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("queue declare failed");

        (container, channel, queue_name)
    }

    async fn fetch_message(channel: &Channel, queue: &str) -> lapin::message::BasicGetMessage {
        let timeout = Duration::from_secs(3);
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout {
                panic!("Timed out waiting for message");
            }

            let result = channel
                .basic_get(queue, BasicGetOptions::default())
                .await
                .expect("basic_get failed");

            if let Some(delivery) = result {
                return delivery;
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    fn create_test_parquet_file(docs: &[TextDocument]) -> Result<NamedTempFile> {
        let file = NamedTempFile::new().unwrap();
        let mut writer = ParquetWriter::new(file.path().to_str().unwrap())?;
        writer.write_batch(docs)?;
        writer.close()?;
        Ok(file)
    }

    #[ignore]
    #[tokio::test]
    async fn test_publish_tasks_single_document() -> Result<()> {
        let (_container, channel, queue_name) = start_rabbitmq_container().await;

        let doc = TextDocument {
            id: "doc-1".into(),
            source: "test".into(),
            content: "Simple content".into(),
            metadata: [("lang".into(), "en".into())].into(),
            ..Default::default()
        };
        let parquet: NamedTempFile = create_test_parquet_file(&[doc])?;

        let args = create_mock_args(
            parquet.path().to_str().unwrap().to_string(),
            queue_name.clone(),
        );

        let pb = ProgressBar::hidden();
        let result = publish_tasks(&args, &channel, &pb).await?;
        assert_eq!(result, 1);

        let delivery = fetch_message(&channel, &queue_name).await;
        let value: serde_json::Value = serde_json::from_slice(&delivery.data)?;
        assert_eq!(value["id"], "doc-1");
        assert_eq!(value["content"], "Simple content");
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_publish_tasks_multiple_documents() -> Result<()> {
        let (_container, channel, queue_name) = start_rabbitmq_container().await;

        let docs = vec![
            TextDocument {
                id: "a".into(),
                source: "s".into(),
                content: "1".into(),
                metadata: HashMap::new(),
                ..Default::default()
            },
            TextDocument {
                id: "b".into(),
                source: "s".into(),
                content: "2".into(),
                metadata: HashMap::new(),
                ..Default::default()
            },
            TextDocument {
                id: "c".into(),
                source: "s".into(),
                content: "3".into(),
                metadata: HashMap::new(),
                ..Default::default()
            },
        ];
        let parquet = create_test_parquet_file(&docs)?;

        let args = create_mock_args(
            parquet.path().to_str().unwrap().to_string(),
            queue_name.clone(),
        );

        let pb = ProgressBar::hidden();
        let result = publish_tasks(&args, &channel, &pb).await?;
        assert_eq!(result, 3);

        let mut seen_ids = vec![];
        for _ in 0..3 {
            let d = fetch_message(&channel, &queue_name).await;
            let val: serde_json::Value = serde_json::from_slice(&d.data)?;
            seen_ids.push(val["id"].as_str().unwrap().to_string());
        }

        assert_eq!(seen_ids.len(), 3);
        assert!(seen_ids.contains(&"a".to_string()));
        assert!(seen_ids.contains(&"b".to_string()));
        assert!(seen_ids.contains(&"c".to_string()));
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_publish_tasks_empty_metadata() -> Result<()> {
        let (_container, channel, queue_name) = start_rabbitmq_container().await;

        let doc = TextDocument {
            id: "empty-meta".into(),
            source: "unit".into(),
            content: "Testing empty metadata".into(),
            metadata: HashMap::new(),
            ..Default::default()
        };

        let parquet = create_test_parquet_file(&[doc])?;

        let args = create_mock_args(
            parquet.path().to_str().unwrap().to_string(),
            queue_name.clone(),
        );

        let pb = ProgressBar::hidden();
        let result = publish_tasks(&args, &channel, &pb).await?;
        assert_eq!(result, 1);

        let delivery = fetch_message(&channel, &queue_name).await;
        let val: serde_json::Value = serde_json::from_slice(&delivery.data)?;
        assert_eq!(val["id"], "empty-meta");
        Ok(())
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
            prefetch_count: 10,
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
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_aggregate_results_success() {
        let doc = TextDocument {
            id: "test-id".to_string(),
            content: "Test document".to_string(),
            source: "test-src".to_string(),
            metadata: HashMap::new(),
            ..Default::default()
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
