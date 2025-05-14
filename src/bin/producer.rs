// src/bin/producer.rs

use clap::Parser;
use futures::StreamExt; // For consuming the results stream
use indicatif::{HumanDuration, ProgressBar, ProgressStyle}; // Added indicatif
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    protocol::basic::AMQPProperties,
    types::FieldTable,
    Connection, ConnectionProperties, Result as LapinResult,
};
use std::time::{Duration, Instant}; // Added Instant
use tokio::time::sleep;
use tracing::{error, info, warn}; // Added tracing
use tracing_subscriber::{fmt, EnvFilter}; // Added tracing_subscriber
use TextBlaster::config::ParquetInputConfig;
use TextBlaster::data_model::{ProcessingOutcome, TextDocument}; // Import both TextDocument and ProcessingOutcome
use TextBlaster::error::{PipelineError, Result}; // Use the library's Result type
use TextBlaster::pipeline::readers::ParquetReader;
use TextBlaster::pipeline::writers::parquet_writer::ParquetWriter; // For retry delay

const PARQUET_WRITE_BATCH_SIZE: usize = 500; // Configurable batch size for writing

// Define command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the input Parquet file
    #[arg(short, long)]
    input_file: String,

    /// Text column name in the Parquet file
    #[arg(long, default_value = "text")]
    text_column: String,

    /// Optional ID column name in the Parquet file
    #[arg(long)]
    id_column: Option<String>,

    /// RabbitMQ connection string (e.g., amqp://guest:guest@localhost:5672/%2f)
    #[arg(short, long, default_value = "amqp://guest:guest@localhost:5672/%2f")]
    amqp_addr: String,

    /// Name of the queue to publish tasks to
    #[arg(short = 'q', long, default_value = "task_queue")]
    task_queue: String,

    /// Name of the queue to consume results/outcomes from
    #[arg(short = 'r', long, default_value = "results_queue")]
    results_queue: String,

    /// Path to the output Parquet file
    #[arg(short = 'o', long, default_value = "output_processed.parquet")]
    output_file: String,

    /// Path to the excluded output Parquet file
    #[arg(short = 'e', long, default_value = "excluded.parquet")]
    excluded_file: String,

    /// Optional: Port for the Prometheus metrics HTTP endpoint
    #[arg(long)]
    metrics_port: Option<u16>,
}

// --- Prometheus Metrics ---
use once_cell::sync::Lazy; // Using once_cell as it's already a dependency
use prometheus::{
    register_counter, register_gauge, register_histogram, Counter, Encoder, Gauge, Histogram,
    TextEncoder,
};

static TASKS_PUBLISHED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_tasks_published_total",
        "Total number of tasks published."
    )
    .expect("Failed to register TASKS_PUBLISHED_TOTAL counter")
});

static TASK_PUBLISH_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_task_publish_errors_total",
        "Total number of errors during task publishing (serialization, broker ack)."
    )
    .expect("Failed to register TASK_PUBLISH_ERRORS_TOTAL counter")
});

static RESULTS_RECEIVED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_received_total",
        "Total number of results/outcomes received."
    )
    .expect("Failed to register RESULTS_RECEIVED_TOTAL counter")
});

static RESULTS_SUCCESS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_success_total",
        "Total number of successful results."
    )
    .expect("Failed to register RESULTS_SUCCESS_TOTAL counter")
});

static RESULTS_FILTERED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_filtered_total",
        "Total number of filtered results."
    )
    .expect("Failed to register RESULTS_FILTERED_TOTAL counter")
});

static RESULT_DESERIALIZATION_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_result_deserialization_errors_total",
        "Total number of errors deserializing results."
    )
    .expect("Failed to register RESULT_DESERIALIZATION_ERRORS_TOTAL counter")
});

static ACTIVE_TASKS_IN_FLIGHT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "producer_active_tasks_in_flight",
        "Number of tasks published but not yet resolved."
    )
    .expect("Failed to register ACTIVE_TASKS_IN_FLIGHT gauge")
});

static TASK_PUBLISHING_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "producer_task_publishing_duration_seconds",
        "Histogram of task publishing latencies (from send to broker ack)."
    )
    .expect("Failed to register TASK_PUBLISHING_DURATION_SECONDS histogram")
});

// Axum handler for /metrics
async fn metrics_handler() -> (axum::http::StatusCode, String) {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("Could not encode prometheus metrics: {}", e); // Changed to error!
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not encode prometheus metrics: {}", e),
        );
    }
    match String::from_utf8(buffer) {
        Ok(s) => (axum::http::StatusCode::OK, s),
        Err(e) => {
            error!("Prometheus metrics UTF-8 error: {}", e); // Changed to error!
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Prometheus metrics UTF-8 error: {}", e),
            )
        }
    }
}

// Helper function to connect to RabbitMQ with retry
async fn connect_rabbitmq(addr: &str) -> LapinResult<Connection> {
    let options = ConnectionProperties::default()
        // Use tokio executor and reactor:
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);

    // Retry connection logic
    let mut attempts = 0;
    loop {
        match Connection::connect(addr, options.clone()).await {
            Ok(conn) => {
                info!("Successfully connected to RabbitMQ at {}", addr); // Changed to info!
                return Ok(conn);
            }
            Err(e) => {
                attempts += 1;
                error!( // Changed to error!
                    attempt = attempts,
                    error = %e,
                    "Failed to connect to RabbitMQ. Retrying in 5 seconds..."
                );
                if attempts >= 5 {
                    // Limit retries
                    return Err(e); // Return the last error after retries
                }
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// Helper function for creating a styled progress bar
fn create_progress_bar(total_items: u64, message: &str, template: &str) -> ProgressBar {
    let pb = if total_items == 0 {
        // Spinner if total is unknown (or 0)
        ProgressBar::new_spinner()
    } else {
        ProgressBar::new(total_items)
    };
    pb.set_message(message.to_string());
    pb.set_style(
        ProgressStyle::default_bar()
            .template(template)
            .unwrap_or_else(|_| ProgressStyle::default_bar()) // Fallback style
            .progress_chars("=> "),
    );
    pb
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize tracing subscriber
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set
    fmt::Subscriber::builder().with_env_filter(filter).init();

    // --- Optional: Start Metrics Endpoint ---
    if let Some(port) = args.metrics_port {
        let app = axum::Router::new().route("/metrics", axum::routing::get(metrics_handler));
        let listener_addr = format!("0.0.0.0:{}", port);
        info!(
            "Metrics endpoint will be available at http://{}/metrics",
            listener_addr
        );

        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(&listener_addr).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, app).await {
                        error!("Metrics server error: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to bind metrics server to {}: {}", listener_addr, e);
                }
            }
        });
    }

    info!("Producer started.");
    info!("Input file: {}", args.input_file);
    info!("Task Queue: {} @ {}", args.task_queue, args.amqp_addr);
    info!("Results/Outcomes Queue: {}", args.results_queue);
    info!("Output File: {}", args.output_file);

    // 1. Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr).await?;
    let publish_channel = conn.create_channel().await?; // Channel for publishing tasks

    // 2. Declare the task queue (durable)
    let _task_queue_info = publish_channel
        .queue_declare(
            &args.task_queue,
            QueueDeclareOptions {
                durable: true, // Ensure tasks survive broker restart
                ..Default::default()
            },
            Default::default(),
        )
        .await?;
    info!("Declared durable task queue '{}'", args.task_queue);

    // 3. Configure and create the Parquet Reader
    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone(),
        batch_size: Some(1024), // Example batch size for reading
    };
    let reader = ParquetReader::new(parquet_config);

    // 4. Read documents and publish to the task queue
    info!("Reading documents and publishing tasks..."); // This was already info!
    let publishing_pb_template =
        "{spinner:.green} [{elapsed_precise}] {msg} Tasks published: {pos} ({per_sec}, ETA: {eta})";
    let publishing_pb = create_progress_bar(0, "Publishing tasks", publishing_pb_template); // 0 for spinner

    let mut published_count = 0u64; // Use u64 for potentially large counts
    let mut read_errors = 0u64; // This counts errors before attempting to publish
    let doc_iterator = reader.read_documents()?;
    let publish_start_time = Instant::now();

    for doc_result in doc_iterator {
        publishing_pb.tick(); // Keep spinner active
        match doc_result {
            Ok(doc) => {
                match serde_json::to_vec(&doc) {
                    Ok(payload) => {
                        let task_publish_timer = TASK_PUBLISHING_DURATION_SECONDS.start_timer();
                        // Publish task persistently
                        let publish_confirm = publish_channel
                            .basic_publish(
                                "", // Default exchange
                                &args.task_queue,
                                BasicPublishOptions::default(),
                                &payload,
                                AMQPProperties::default().with_delivery_mode(2), // 2 = Persistent
                            )
                            .await? // Initiates publish, returns confirmation future
                            .await; // Waits for broker ack/nack
                        task_publish_timer.observe_duration();

                        match publish_confirm {
                            Ok(_) => {
                                published_count += 1;
                                TASKS_PUBLISHED_TOTAL.inc();
                                ACTIVE_TASKS_IN_FLIGHT.inc();
                                publishing_pb.inc(1); // Increment after successful publish
                            }
                            Err(e) => {
                                TASK_PUBLISH_ERRORS_TOTAL.inc();
                                publishing_pb.println(format!(
                                    "FATAL: Failed broker confirmation for task (Doc ID {}): {}. Stopping.",
                                    doc.id, e
                                ));
                                error!( // Changed from eprintln!
                                    doc_id = %doc.id,
                                    error = %e,
                                    "FATAL: Failed broker confirmation for task. Stopping."
                                );
                                publishing_pb
                                    .finish_with_message(format!("Publishing failed: {}", e));
                                return Err(PipelineError::QueueError(format!(
                                    "Publish confirmation failed: {}",
                                    e
                                )));
                            }
                        }
                    }
                    Err(e) => {
                        TASK_PUBLISH_ERRORS_TOTAL.inc(); // Count serialization errors as publish errors
                        publishing_pb.println(format!(
                            "Failed to serialize task (Doc ID {}): {}. Skipping.",
                            doc.id, e
                        ));
                        read_errors += 1; // Keep this for the summary print
                    }
                }
            }
            Err(e) => {
                // These are errors reading from the source, not directly publish errors yet
                publishing_pb.println(format!("Error reading document for task: {}. Skipping.", e));
                read_errors += 1;
            }
        }
    }
    let publishing_duration = publish_start_time.elapsed();
    publishing_pb.finish_with_message(format!(
        "Finished publishing {} tasks in {}. Read/Serialization Errors: {}",
        published_count,
        HumanDuration(publishing_duration),
        read_errors
    ));

    // Early exit if no tasks were published (nothing to wait for)
    if published_count == 0 {
        info!("No tasks were published. Exiting."); // This was already info!
                                                    // Close channel/connection before exiting
                                                    // let _ = publish_channel.close(200, "No tasks published").await;
                                                    // let _ = conn.close(200, "No tasks published").await;
        return Ok(());
    }

    // --- Phase 2: Consume results/outcomes and write to Parquet ---
    info!("\nStarting results aggregation phase..."); // This was already info!

    // 5. Create Channel and Declare Results Queue
    let consume_channel = conn.create_channel().await?; // New channel for consuming outcomes
    let _results_queue_info = consume_channel
        .queue_declare(
            &args.results_queue,
            QueueDeclareOptions {
                durable: true, // Should match worker's declaration for persistence
                ..Default::default()
            },
            Default::default(),
        )
        .await?;
    info!("Declared durable results queue '{}'", args.results_queue); // This was already info!

    // 6. Initialize Parquet Writer
    // Ensure the directory for the output file exists
    if let Some(parent_dir) = std::path::Path::new(&args.output_file).parent() {
        tokio::fs::create_dir_all(parent_dir).await?; // Create parent dirs if needed
    }
    let mut parquet_writer_output = ParquetWriter::new(&args.output_file)?;
    info!("Initialized Parquet writer for: {}", args.output_file); // This was already info!

    let mut parquet_writer_excluded = ParquetWriter::new(&args.excluded_file)?;
    info!("Initialized Parquet writer for: {}", args.excluded_file); // This was already info!

    // 7. Start Consuming Outcomes
    let mut results_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut excluded_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut outcomes_received_count = 0u64; // Count total outcomes (Success + Filtered)
    let mut success_count = 0u64; // Count only successful documents
    let mut filtered_count = 0u64; // Count filtered documents
    let mut outcome_deserialization_errors = 0u64;

    let consumer_tag = format!(
        "producer-aggregator-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp()
    );
    let mut consumer = consume_channel
        .basic_consume(
            &args.results_queue,
            &consumer_tag,
            BasicConsumeOptions::default(), // auto_ack: false (default)
            FieldTable::default(),
        )
        .await?;

    info!(
        // This was already info!
        "Waiting for outcomes from queue '{}'. Expecting {} outcomes (Success or Filtered).",
        args.results_queue, published_count
    );

    let aggregation_pb_template = "{spinner:.blue} [{elapsed_precise}] {msg} Results received: {pos}/{len} ({percent}%) ({per_sec}, ETA: {eta})";
    let aggregation_pb = create_progress_bar(
        published_count,
        "Aggregating results",
        aggregation_pb_template,
    );
    let aggregation_start_time = Instant::now();

    // Loop until we have received an outcome for every task published
    while outcomes_received_count < published_count {
        aggregation_pb.tick(); // Keep spinner active if it's a spinner (though here it's not)
        let delivery_result = consumer.next().await;

        if delivery_result.is_none() {
            // Stream closed unexpectedly (e.g., connection lost)
            aggregation_pb.println(
                "ERROR: Consumer stream closed unexpectedly before receiving all outcomes.",
            );
            break; // Exit the loop
        }

        match delivery_result.unwrap() {
            // We checked for None above
            Ok(delivery) => {
                // Deserialize the received outcome message
                match serde_json::from_slice::<ProcessingOutcome>(&delivery.data) {
                    Ok(outcome) => {
                        outcomes_received_count += 1; // Increment for ANY valid outcome
                        RESULTS_RECEIVED_TOTAL.inc();
                        aggregation_pb.inc(1);

                        match outcome {
                            ProcessingOutcome::Success(doc) => {
                                success_count += 1;
                                RESULTS_SUCCESS_TOTAL.inc();
                                results_batch.push(doc);
                                // Write batch if full
                                if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    match parquet_writer_output.write_batch(&results_batch) {
                                        Ok(_) => {
                                            aggregation_pb.println(format!(
                                                "Written Parquet batch ({} docs). Total outcomes: {}/{}, Success: {}, Filtered: {}",
                                                results_batch.len(), outcomes_received_count, published_count, success_count, filtered_count
                                            ));
                                            results_batch.clear(); // Clear batch after successful write
                                        }
                                        Err(e) => {
                                            // Consider metric for Parquet write errors
                                            aggregation_pb.println(format!("FATAL: Failed to write Parquet batch: {}. Stopping.", e));
                                            let _ = delivery.ack(BasicAckOptions::default()).await;
                                            aggregation_pb.finish_with_message(
                                                "Aggregation failed due to Parquet write error.",
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                            ProcessingOutcome::Filtered {
                                document,
                                reason: _,
                            } => {
                                filtered_count += 1;
                                RESULTS_FILTERED_TOTAL.inc();
                                excluded_batch.push(document);
                                // Write batch if full
                                if excluded_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    match parquet_writer_excluded.write_batch(&excluded_batch) {
                                        Ok(_) => {
                                            aggregation_pb.println(format!(
                                                "Written Filtered Parquet batch ({} docs). Total outcomes: {}/{}, Success: {}, Filtered: {}",
                                                excluded_batch.len(), outcomes_received_count, published_count, success_count, filtered_count
                                            ));
                                            excluded_batch.clear(); // Clear batch after successful write
                                        }
                                        Err(e) => {
                                            // Consider metric for Parquet write errors
                                            aggregation_pb.println(format!("FATAL: Failed to write Filtered Parquet batch: {}. Stopping.", e));
                                            let _ = delivery.ack(BasicAckOptions::default()).await;
                                            aggregation_pb.finish_with_message("Aggregation failed due to Parquet write error (filtered).");
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        outcome_deserialization_errors += 1;
                        RESULT_DESERIALIZATION_ERRORS_TOTAL.inc();
                        // It's an outcome, so decrement in-flight tasks even if we can't parse it.
                        // Otherwise, the in-flight count will be wrong if poison pills occur.
                        ACTIVE_TASKS_IN_FLIGHT.dec();
                        aggregation_pb.println(format!(
                            "Failed to deserialize outcome message {}: {}. Payload: {:?}",
                            delivery.delivery_tag,
                            e,
                            std::str::from_utf8(&delivery.data).unwrap_or("[invalid utf8]")
                        ));
                    }
                }

                if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                    // Consider metric for ACK errors
                    aggregation_pb.println(format!(
                        "Failed to ack outcome message {}: {}. This might lead to duplicate processing counts.",
                        delivery.delivery_tag, ack_err
                    ));
                }
            }
            Err(e) => {
                aggregation_pb.println(format!(
                    "Error receiving outcome message from consumer stream: {}",
                    e
                ));
                aggregation_pb
                    .finish_with_message("Aggregation failed due to consumer stream error.");
                break;
            }
        }
    }
    let aggregation_duration = aggregation_start_time.elapsed();
    aggregation_pb.finish_with_message(format!(
        "Finished consuming outcomes (Received {}/{} expected) in {}.",
        outcomes_received_count,
        published_count,
        HumanDuration(aggregation_duration)
    ));

    // 8. Write any remaining documents in the final batch
    if !results_batch.is_empty() {
        info!(
            count = results_batch.len(),
            "Writing final batch of successfully processed documents..."
        );
        match parquet_writer_output.write_batch(&results_batch) {
            Ok(_) => {
                info!("Final batch written successfully.");
            }
            Err(e) => {
                error!(error = %e, "ERROR: Failed to write final Parquet batch");
                // File might be incomplete/corrupted here
            }
        }
    }

    // 8. Write any remaining documents in the final batch
    if !excluded_batch.is_empty() {
        info!(
            count = excluded_batch.len(),
            "Writing final batch of excluded documents..."
        );
        match parquet_writer_excluded.write_batch(&excluded_batch) {
            Ok(_) => {
                info!("Final excluded batch written successfully.");
            }
            Err(e) => {
                error!(error = %e, "ERROR: Failed to write final excluded Parquet batch");
                // File might be incomplete/corrupted here
            }
        }
    }

    // 9. Close the Parquet writer (essential!)
    info!("Closing Parquet writer for output_processed.parquet...");
    if let Err(e) = parquet_writer_output.close() {
        error!(error = %e, "ERROR: Failed to close Parquet writer (output_processed.parquet) cleanly");
    } else {
        info!("Parquet writer (output_processed.parquet) closed successfully.");
    }

    info!("Closing Parquet writer for excluded.parquet...");
    if let Err(e) = parquet_writer_excluded.close() {
        error!(error = %e, "ERROR: Failed to close Parquet writer (excluded.parquet) cleanly");
    } else {
        info!("Parquet writer (excluded.parquet) closed successfully.");
    }

    // 10. Final Summary
    info!("--------------------");
    info!("Processing Summary:");
    info!("  Tasks Published: {}", published_count);
    info!("  Task Read/Serialization Errors: {}", read_errors);
    info!("  Outcomes Received (Total): {}", outcomes_received_count);
    info!("    - Successfully Processed: {}", success_count);
    info!("    - Filtered by Worker: {}", filtered_count);
    info!(
        "  Outcome Deserialization Errors: {}",
        outcome_deserialization_errors
    );
    info!("  Output File: {}", args.output_file);
    info!("  Excluded File: {}", args.excluded_file);
    info!("--------------------");

    // Optional: Graceful shutdown of channels and connection
    // let _ = consume_channel.close(200, "Producer finished aggregation").await;
    // let _ = publish_channel.close(200, "Producer finished publishing").await;
    // let _ = conn.close(200, "Producer finished").await;

    // Final check for mismatch
    if outcomes_received_count != published_count {
        warn!(
            outcomes_received = outcomes_received_count,
            tasks_published = published_count,
            "Warning: Number of outcomes received does not match tasks published. Output might be incomplete or counts inaccurate due to errors or message loss."
        );
    }

    Ok(())
}
