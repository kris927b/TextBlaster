use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use rand::Rng;
use tempfile::tempdir;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage,
}; // Added AsyncRunner

use TextBlaster::config::parquet::ParquetInputConfig;
use TextBlaster::data_model::TextDocument;
use TextBlaster::error::Result; // Assuming this is your crate's Result type
use TextBlaster::pipeline::readers::parquet_reader::ParquetReader;
use TextBlaster::pipeline::writers::parquet_writer::ParquetWriter;

// Helper to create TextDocument instances easily
fn create_doc(id: &str, content: &str, source: &str, lang: Option<&str>) -> TextDocument {
    let mut metadata = HashMap::new();
    if let Some(l) = lang {
        metadata.insert("language".to_string(), l.to_string());
    }
    TextDocument {
        id: id.to_string(),
        content: content.to_string(),
        source: source.to_string(),
        metadata,
    }
}

// Helper function to create a test input Parquet file
fn create_test_input_parquet_file(
    docs: &[TextDocument],
    dir: &Path,
    filename: &str,
) -> Result<PathBuf> {
    let file_path = dir.join(filename);
    let mut writer = ParquetWriter::new(file_path.to_str().expect("Path should be valid UTF-8"))?;
    writer.write_batch(docs)?;
    writer.close()?;
    Ok(file_path)
}

// Helper function to start a RabbitMQ container
async fn start_rabbitmq_container() -> (ContainerAsync<GenericImage>, String) {
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

    (container, amqp_addr)
}

#[tokio::test]
#[ignore] // Ignore by default, as it requires Docker and can be slow
async fn test_full_pipeline_e2e() -> Result<()> {
    // 1. Generate a unique ID for this test run
    let test_id: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    println!("Running test_full_pipeline_e2e with ID: {}", test_id);

    // 2. Create temporary directories for test data
    let base_temp_dir = tempdir().expect("Failed to create base temp dir");
    let input_dir = base_temp_dir.path().join("input");
    let output_dir = base_temp_dir.path().join("output");
    fs::create_dir_all(&input_dir).expect("Failed to create input dir");
    fs::create_dir_all(&output_dir).expect("Failed to create output dir");

    // 3. Start the RabbitMQ container
    println!("Starting RabbitMQ container for test ID: {}...", test_id);
    let (_rabbitmq_container, amqp_addr) = start_rabbitmq_container().await;
    println!("RabbitMQ container started. AMQP address: {}", amqp_addr);

    // 4. Define unique task and result queue names
    let task_queue_name = format!("task_queue_{}", test_id);
    let results_queue_name = format!("results_queue_{}", test_id);
    println!(
        "Task Queue: {}, Results Queue: {}",
        task_queue_name, results_queue_name
    );

    // 5. Create a sample input TextDocument list
    let doc1_pass = create_doc(
        "doc1_en",
        "Sometimes, all you need to start the day right is a good coffee and someone greeting you smiling.",
        "test_source",
        Some("eng"), // Will be kept by LanguageDetectionFilter
    );
    let doc2_filter = create_doc(
        "doc2_fr",
        "Ceci est un document en français.", // French content
        "test_source",
        Some("fr"), // Will be filtered by LanguageDetectionFilter expecting 'en'
    );
    let doc3_pass_no_lang_hint = create_doc(
        "doc3_en_implicit",
        "Another valid English text without any specific language hint in metadata.",
        "test_source",
        None, // Language detection should still pick it up as 'en'
    );

    let input_docs = vec![
        doc1_pass.clone(),
        doc2_filter.clone(),
        doc3_pass_no_lang_hint.clone(),
    ];

    // 6. Create the input Parquet file
    let input_parquet_file = create_test_input_parquet_file(
        &input_docs,
        &input_dir,
        &format!("input_{}.parquet", test_id),
    )?;
    println!("Input Parquet file created: {:?}", input_parquet_file);

    // 7. Construct paths for the output and excluded Parquet files
    let output_parquet_file = output_dir.join(format!("processed_output_{}.parquet", test_id));
    let excluded_parquet_file = output_dir.join(format!("excluded_output_{}.parquet", test_id));
    println!("Output Parquet file path: {:?}", output_parquet_file);
    println!("Excluded Parquet file path: {:?}", excluded_parquet_file);

    // 5. Execute Producer and Worker Processes

    // Path to the compiled binaries (target/debug/producer and target/debug/worker)
    // This assumes `cargo test` is run from the project root.
    let target_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug");
    let producer_exe = target_dir.join("producer");
    let worker_exe = target_dir.join("worker");
    let test_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("config")
        .join("test_pipeline_config.yaml");

    println!("Producer executable: {:?}", producer_exe);
    println!("Worker executable: {:?}", worker_exe);
    println!("Test config path: {:?}", test_config_path);

    // Ensure executables exist
    assert!(
        producer_exe.exists(),
        "Producer executable not found at {:?}",
        producer_exe
    );
    assert!(
        worker_exe.exists(),
        "Worker executable not found at {:?}",
        worker_exe
    );
    assert!(
        test_config_path.exists(),
        "Test config file not found at {:?}",
        test_config_path
    );

    // Start Worker
    println!("Starting worker process for test ID: {}...", test_id);
    let mut worker_process = Command::new(&worker_exe)
        .arg("--amqp-addr")
        .arg(&amqp_addr)
        .arg("--task-queue")
        .arg(&task_queue_name)
        .arg("--results-queue")
        .arg(&results_queue_name)
        .arg("--pipeline-config")
        .arg(test_config_path.to_str().unwrap())
        // .arg("--metrics-port") // Optionally add if needed for the test or to avoid port conflicts
        // .arg("0") // Request a random available port for metrics if enabled
        .stdout(Stdio::piped()) // Capture stdout
        .stderr(Stdio::piped()) // Capture stderr
        .spawn()
        .expect("Failed to spawn worker process");
    println!("Worker process spawned (PID: {}).", worker_process.id());

    // Give the worker a moment to start up and connect to RabbitMQ
    // This is a common practice in integration tests involving message queues.
    // A more robust solution would be to check RabbitMQ management API for consumer count,
    // or have the worker signal readiness, but sleep is simpler for now.
    tokio::time::sleep(Duration::from_secs(5)).await; // Using tokio::time::sleep

    // Start Producer
    println!("Starting producer process for test ID: {}...", test_id);
    let producer_process = Command::new(&producer_exe)
        .arg("--input-file")
        .arg(input_parquet_file.to_str().unwrap())
        .arg("--text-column")
        .arg("content")
        .arg("--id-column") // Add this line
        .arg("id") // Add this line
        .arg("--amqp-addr")
        .arg(&amqp_addr)
        .arg("--task-queue")
        .arg(&task_queue_name)
        .arg("--results-queue")
        .arg(&results_queue_name)
        .arg("--output-file")
        .arg(output_parquet_file.to_str().unwrap())
        .arg("--excluded-file")
        .arg(excluded_parquet_file.to_str().unwrap())
        // .arg("--metrics-port")
        // .arg("0")
        .stdout(Stdio::piped()) // Capture stdout
        .stderr(Stdio::piped()) // Capture stderr
        .spawn()
        .expect("Failed to spawn producer process");
    println!("Producer process spawned (PID: {}).", producer_process.id());

    // 6. Wait for Completion and Verify Results
    println!(
        "Waiting for producer process (PID: {}) to complete...",
        producer_process.id()
    );
    let producer_output = producer_process
        .wait_with_output()
        .expect("Failed to wait for producer process");

    println!("Producer finished.");
    println!("Producer stdout:");
    println!("{}", String::from_utf8_lossy(&producer_output.stdout));
    println!("Producer stderr:");
    eprintln!("{}", String::from_utf8_lossy(&producer_output.stderr)); // Use eprintln for stderr
    assert!(
        producer_output.status.success(),
        "Producer process exited with an error. Check stderr above."
    );

    // Handle worker process
    // Try to wait for the worker for a bit, then kill if it hasn't exited.
    // The producer finishing should mean all work is done and results sent.
    println!("Handling worker process (PID: {})...", worker_process.id());
    match worker_process.try_wait() {
        Ok(Some(status)) => {
            println!("Worker process exited on its own with status: {}", status);
            let worker_final_output = worker_process
                .wait_with_output()
                .expect("Failed to get worker output after it exited");
            println!(
                "Worker stdout:\n{}",
                String::from_utf8_lossy(&worker_final_output.stdout)
            );
            eprintln!(
                "Worker stderr:\n{}",
                String::from_utf8_lossy(&worker_final_output.stderr)
            ); // Use eprintln for stderr
        }
        Ok(None) => {
            println!("Worker process still running, attempting to kill it.");
            if let Err(e) = worker_process.kill() {
                eprintln!(
                    "Failed to kill worker process: {}. It might have already exited.",
                    e
                );
            } else {
                println!("Worker process killed.");
            }
            // Always attempt to get output after it has been killed or exited.
            let worker_final_output = worker_process
                .wait_with_output()
                .expect("Failed to get output from worker after kill/exit attempt");
            println!(
                "Worker stdout (after kill/exit attempt):\n{}",
                String::from_utf8_lossy(&worker_final_output.stdout)
            );
            eprintln!(
                "Worker stderr (after kill/exit attempt):\n{}",
                String::from_utf8_lossy(&worker_final_output.stderr)
            ); // Use eprintln for stderr
        }
        Err(e) => {
            eprintln!("Error trying to wait for worker process: {}", e);
            // Attempt to get output anyway if try_wait fails, though it might also fail.
            if let Ok(worker_final_output) = worker_process.wait_with_output() {
                println!(
                    "Worker stdout (after try_wait error):\n{}",
                    String::from_utf8_lossy(&worker_final_output.stdout)
                );
                eprintln!(
                    "Worker stderr (after try_wait error):\n{}",
                    String::from_utf8_lossy(&worker_final_output.stderr)
                );
            }
        }
    }

    // Verify the output Parquet file
    println!("Verifying output Parquet file: {:?}", output_parquet_file);
    assert!(
        output_parquet_file.exists(),
        "Output Parquet file was not created."
    );
    let output_reader_config = ParquetInputConfig {
        path: output_parquet_file.to_str().unwrap().to_string(),
        text_column: "content".to_string(),
        id_column: Some("id".to_string()),
        batch_size: Some(10),
    };
    let output_reader = ParquetReader::new(output_reader_config);
    let mut processed_docs: Vec<TextDocument> = output_reader
        .read_documents()?
        .collect::<Result<Vec<_>>>()?;

    // Verify the excluded Parquet file
    println!(
        "Verifying excluded Parquet file: {:?}",
        excluded_parquet_file
    );
    assert!(
        excluded_parquet_file.exists(),
        "Excluded Parquet file was not created."
    );
    let excluded_reader_config = ParquetInputConfig {
        path: excluded_parquet_file.to_str().unwrap().to_string(),
        text_column: "content".to_string(),
        id_column: Some("id".to_string()),
        batch_size: Some(10),
    };
    let excluded_reader = ParquetReader::new(excluded_reader_config);
    let mut filtered_docs: Vec<TextDocument> = excluded_reader
        .read_documents()?
        .collect::<Result<Vec<_>>>()?;

    // Sort documents by ID for consistent comparison
    processed_docs.sort_by(|a, b| a.id.cmp(&b.id));
    filtered_docs.sort_by(|a, b| a.id.cmp(&b.id));

    println!(
        "Processed Docs IDs (debug): {:#?}",
        processed_docs.iter().map(|d| &d.id).collect::<Vec<_>>()
    );
    println!(
        "Filtered Docs IDs (debug): {:#?}",
        filtered_docs.iter().map(|d| &d.id).collect::<Vec<_>>()
    );
    println!("Expected doc1_pass ID: {:?}", doc1_pass.id);

    // Assertions
    assert_eq!(
        processed_docs.len(),
        2,
        "Expected 2 documents in the output file."
    );
    assert_eq!(
        filtered_docs.len(),
        1,
        "Expected 1 document in the excluded file."
    );

    // Check content of processed docs
    // Note: ParquetReader sets the 'source' field to the file path. We should not compare it directly with original 'source'.
    // Metadata might also be affected by processing or Parquet read/write. Focus on ID and content.

    let expected_doc1_pass = processed_docs.iter().find(|d| d.id == doc1_pass.id);
    assert!(
        expected_doc1_pass.is_some(),
        "doc1_pass (doc1_en) not found in processed docs"
    );
    assert_eq!(
        expected_doc1_pass.unwrap().content,
        doc1_pass.content,
        "Content mismatch for doc1_pass"
    );

    let expected_doc3_pass = processed_docs
        .iter()
        .find(|d| d.id == doc3_pass_no_lang_hint.id);
    assert!(
        expected_doc3_pass.is_some(),
        "doc3_pass_no_lang_hint (doc3_en_implicit) not found in processed docs"
    );
    assert_eq!(
        expected_doc3_pass.unwrap().content,
        doc3_pass_no_lang_hint.content,
        "Content mismatch for doc3_pass_no_lang_hint"
    );

    // Check content of filtered docs
    let expected_doc2_filter = filtered_docs.iter().find(|d| d.id == doc2_filter.id);
    assert!(
        expected_doc2_filter.is_some(),
        "doc2_filter (doc2_fr) not found in filtered docs"
    );
    assert_eq!(
        expected_doc2_filter.unwrap().content,
        doc2_filter.content,
        "Content mismatch for doc2_filter"
    );

    // Ensure original documents are still in scope for comparison or clone them earlier.
    // The `doc1_pass.clone(), doc2_filter.clone(), doc3_pass_no_lang_hint.clone()` in setup ensures this.

    println!("All assertions passed for test ID: {}", test_id);

    // Cleanup of tempdir is automatic when `_base_temp_dir` goes out of scope.
    // RabbitMQ container is also stopped when `_rabbitmq_container` goes out of scope.
    Ok(())
}
