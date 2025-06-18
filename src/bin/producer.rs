// src/bin/producer.rs

use clap::Parser;
use futures::StreamExt; // For consuming the results stream
use indicatif::{HumanDuration, ProgressBar, ProgressStyle}; // Added indicatif
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    protocol::basic::AMQPProperties,
    types::FieldTable,
};
use std::path::PathBuf; // Added for potential future path ops, not strictly needed now
use std::time::Instant; // Added Instant
use tracing::{error, info, info_span, warn}; // Added tracing & info_span
use tracing_appender; // Added for file logging
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer}; // Added tracing_subscriber components
use TextBlaster::config::ParquetInputConfig;
use TextBlaster::data_model::{ProcessingOutcome, TextDocument}; // Import both TextDocument and ProcessingOutcome
use TextBlaster::error::{PipelineError, Result}; // Use the library's Result type
use TextBlaster::pipeline::readers::ParquetReader;
use TextBlaster::pipeline::writers::parquet_writer::ParquetWriter; // For retry delay
use TextBlaster::utils::utils::{connect_rabbitmq, setup_prometheus_metrics}; // Updated for shared functions
                                                                             // axum and TcpListener imports removed as they are now in utils::utils
use chrono::Utc; // For consumer tag in aggregate_results
use TextBlaster::utils::prometheus_metrics::*; // Import shared metrics

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

// --- Prometheus Metrics (now imported from TextBlaster::utils::prometheus_metrics) ---

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

// Function to publish tasks to RabbitMQ
async fn publish_tasks(
    args: &Args,
    conn: &lapin::Connection,
    publishing_pb: &ProgressBar,
) -> Result<u64> {
    let publish_channel = conn.create_channel().await?;

    // Declare the task queue (durable)
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

    // Configure and create the Parquet Reader
    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone(),
        batch_size: Some(1024), // Example batch size for reading
    };
    let reader = ParquetReader::new(parquet_config);

    info!("Reading documents and publishing tasks...");
    let mut published_count = 0u64;
    let mut read_errors = 0u64; // This counts errors before attempting to publish
    let doc_iterator = reader.read_documents()?;
    let publish_start_time = Instant::now();

    for doc_result in doc_iterator {
        publishing_pb.tick();
        match doc_result {
            Ok(doc) => {
                let _doc_span = info_span!("publishing_doc", doc_id = %doc.id).entered();
                match serde_json::to_vec(&doc) {
                    Ok(payload) => {
                        info!("Serialized document for publishing.");
                        let task_publish_timer = TASK_PUBLISHING_DURATION_SECONDS.start_timer();
                        let publish_confirm = publish_channel
                            .basic_publish(
                                "", // Default exchange
                                &args.task_queue,
                                BasicPublishOptions::default(),
                                &payload,
                                AMQPProperties::default().with_delivery_mode(2), // Persistent
                            )
                            .await?
                            .await; // Wait for broker ack/nack
                        task_publish_timer.observe_duration();

                        match publish_confirm {
                            Ok(_) => {
                                published_count += 1;
                                TASKS_PUBLISHED_TOTAL.inc();
                                ACTIVE_TASKS_IN_FLIGHT.inc();
                                publishing_pb.inc(1);
                                info!("Successfully published task and received confirmation.");
                            }
                            Err(e) => {
                                TASK_PUBLISH_ERRORS_TOTAL.inc();
                                error!(
                                    // doc_id is inherited from span
                                    error = %e,
                                    "FATAL: Failed broker confirmation for task. Stopping."
                                );
                                // The main function will call publishing_pb.finish_with_message on error.
                                return Err(PipelineError::QueueError(format!(
                                    "Publish confirmation failed: {}",
                                    e
                                )));
                            }
                        }
                    }
                    Err(e) => {
                        TASK_PUBLISH_ERRORS_TOTAL.inc();
                        warn!(/* doc_id inherited from span */ error = %e, "Failed to serialize task. Skipping.");
                        read_errors += 1;
                    }
                }
            }
            Err(e) => {
                // doc_id is not available here as reading the document itself failed.
                warn!(error = %e, "Error reading document for task. Skipping.");
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
    Ok(published_count)
}

// Function to aggregate results from RabbitMQ
async fn aggregate_results(
    args: &Args,
    conn: &lapin::Connection,
    published_count: u64,
    aggregation_pb: &ProgressBar,
) -> Result<(u64, u64, u64)> {
    info!("\nStarting results aggregation phase...");

    let consume_channel = conn.create_channel().await?;
    let _results_queue_info = consume_channel
        .queue_declare(
            &args.results_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await?;
    info!("Declared durable results queue '{}'", args.results_queue);

    if let Some(parent_dir) = std::path::Path::new(&args.output_file).parent() {
        tokio::fs::create_dir_all(parent_dir).await?;
    }
    let mut parquet_writer_output = ParquetWriter::new(&args.output_file)?;
    info!("Initialized Parquet writer for: {}", args.output_file);

    let mut parquet_writer_excluded = ParquetWriter::new(&args.excluded_file)?;
    info!("Initialized Parquet writer for: {}", args.excluded_file);

    let mut results_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut excluded_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut outcomes_received_count = 0u64;
    let mut success_count = 0u64;
    let mut filtered_count = 0u64;
    let mut outcome_deserialization_errors = 0u64;

    let consumer_tag = format!(
        "producer-aggregator-{}-{}",
        std::process::id(),
        Utc::now().timestamp()
    );
    let mut consumer = consume_channel
        .basic_consume(
            &args.results_queue,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!(
        "Waiting for outcomes from queue '{}'. Expecting {} outcomes.",
        args.results_queue, published_count
    );
    let aggregation_start_time = Instant::now();

    while outcomes_received_count < published_count {
        aggregation_pb.tick();
        match consumer.next().await {
            Some(Ok(delivery)) => {
                match serde_json::from_slice::<ProcessingOutcome>(&delivery.data) {
                    Ok(outcome) => {
                        outcomes_received_count += 1;
                        RESULTS_RECEIVED_TOTAL.inc();
                        aggregation_pb.inc(1);
                        ACTIVE_TASKS_IN_FLIGHT.dec();

                        match outcome {
                            ProcessingOutcome::Success(doc) => {
                                success_count += 1;
                                RESULTS_SUCCESS_TOTAL.inc();
                                info!(doc_id = %doc.id, "Received successful processing outcome.");
                                results_batch.push(doc);
                                if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    parquet_writer_output.write_batch(&results_batch)?;
                                    info!(
                                        batch_size = results_batch.len(),
                                        total_received = outcomes_received_count,
                                        total_expected = published_count,
                                        success_count = success_count,
                                        filtered_count = filtered_count,
                                        "Written Parquet batch of processed documents."
                                    );
                                    results_batch.clear();
                                }
                            }
                            ProcessingOutcome::Filtered {
                                document,
                                reason,
                            } => {
                                filtered_count += 1;
                                RESULTS_FILTERED_TOTAL.inc();
                                info!(doc_id = %document.id, %reason, "Received filtered processing outcome.");
                                excluded_batch.push(document);
                                if excluded_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    parquet_writer_excluded.write_batch(&excluded_batch)?;
                                    info!(
                                        batch_size = excluded_batch.len(),
                                        total_received = outcomes_received_count,
                                        total_expected = published_count,
                                        success_count = success_count,
                                        filtered_count = filtered_count,
                                        "Written Parquet batch of filtered documents."
                                    );
                                    excluded_batch.clear();
                                }
                            }
                            ProcessingOutcome::Error {
                                document,
                                error_message,
                                worker_id,
                            } => {
                                error!(doc_id = %document.id, worker_id = %worker_id, error = %error_message, "Task processing failed");
                                RESULTS_ERROR_TOTAL.inc();
                            }
                        }
                    }
                    Err(e) => {
                        outcome_deserialization_errors += 1;
                        RESULT_DESERIALIZATION_ERRORS_TOTAL.inc();
                        ACTIVE_TASKS_IN_FLIGHT.dec();
                        warn!(
                            delivery_tag = %delivery.delivery_tag,
                            error = %e,
                            payload = %String::from_utf8_lossy(&delivery.data),
                            "Failed to deserialize outcome."
                        );
                    }
                }
                if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                    error!(
                        delivery_tag = %delivery.delivery_tag,
                        error = %ack_err,
                        "Failed to ack outcome. Might lead to duplicate counts."
                    );
                }
            }
            Some(Err(e)) => {
                error!(error = %e, "Error receiving outcome. Will attempt to finalize current results.");
                break; // Exit loop on consumer error, then try to write remaining batches.
            }
            None => {
                warn!("Consumer stream closed unexpectedly. Will attempt to finalize current results.");
                break; // Exit loop if stream closes, then try to write remaining batches.
            }
        }
    }
    let aggregation_duration = aggregation_start_time.elapsed();
    aggregation_pb.finish_with_message(format!(
        "Finished consuming (Received {}/{}, Desaerial. Errors: {}) in {}.",
        outcomes_received_count,
        published_count,
        outcome_deserialization_errors,
        HumanDuration(aggregation_duration)
    ));

    if !results_batch.is_empty() {
        info!(
            "Writing final batch of successfully processed documents ({} docs)...",
            results_batch.len()
        );
        parquet_writer_output.write_batch(&results_batch)?;
    }
    if !excluded_batch.is_empty() {
        info!(
            "Writing final batch of excluded documents ({} docs)...",
            excluded_batch.len()
        );
        parquet_writer_excluded.write_batch(&excluded_batch)?;
    }

    info!("Closing Parquet writer for output_processed.parquet...");
    parquet_writer_output.close()?;
    info!("Parquet writer (output_processed.parquet) closed successfully.");

    info!("Closing Parquet writer for excluded.parquet...");
    parquet_writer_excluded.close()?;
    info!("Parquet writer (excluded.parquet) closed successfully.");

    Ok((outcomes_received_count, success_count, filtered_count))
}

// setup_prometheus_metrics and metrics_handler removed, now imported from utils

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize tracing subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set

    // Setup file logging
    let file_appender = tracing_appender::rolling::daily("./log", "producer.log");
    let (non_blocking_file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    // Configure the console layer
    let console_layer = fmt::layer()
        .with_writer(std::io::stdout) // Write to stdout
        .with_filter(EnvFilter::new("warn")); // Only info and above for console

    // Configure the file logging layer - JSON formatted
    let file_layer = fmt::layer()
        .with_writer(non_blocking_file_writer) // Write to the file
        .json() // Use JSON formatting
        .with_ansi(false); // No ANSI colors in files

    // Combine layers and initialize the global subscriber
    tracing_subscriber::registry()
        .with(env_filter) // Global filter
        .with(console_layer)
        .with(file_layer)
        .try_init()
        .map_err(|e| {
            PipelineError::ConfigError(format!("Failed to initialize tracing subscriber: {}", e))
        })?;

    // Setup Prometheus Metrics Endpoint
    if let Err(e) = setup_prometheus_metrics(args.metrics_port).await {
        error!("Failed to start Prometheus metrics endpoint: {}", e);
        // Depending on policy, you might want to exit or just log the error.
        // For now, just logging.
    }

    info!("Producer started.");
    info!("Input file: {}", args.input_file);
    info!("Task Queue: {} @ {}", args.task_queue, args.amqp_addr);
    info!("Results/Outcomes Queue: {}", args.results_queue);
    info!("Output File: {}", args.output_file);

    // 1. Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr).await?;

    // --- Progress Bars ---
    let publishing_pb_template =
        "{spinner:.green} [{elapsed_precise}] {msg} Tasks published: {pos} ({per_sec}, ETA: {eta})";
    let publishing_pb = create_progress_bar(0, "Publishing tasks", publishing_pb_template);

    // 2. Publish Tasks
    let published_count = match publish_tasks(&args, &conn, &publishing_pb).await {
        Ok(count) => count,
        Err(e) => {
            error!("Failed during task publishing: {}", e);
            publishing_pb.finish_with_message(format!("Publishing failed: {}", e));
            return Err(e);
        }
    };

    // Early exit if no tasks were published (nothing to wait for)
    if published_count == 0 {
        info!("No tasks were published. Exiting.");
        // Optional: conn.close().await might be useful here if not relying on drop
        return Ok(());
    }

    // --- Progress Bar for Aggregation ---
    let aggregation_pb_template = "{spinner:.blue} [{elapsed_precise}] {msg} Results received: {pos}/{len} ({percent}%) ({per_sec}, ETA: {eta})";
    let aggregation_pb = create_progress_bar(
        published_count,
        "Aggregating results",
        aggregation_pb_template,
    );

    // 3. Aggregate Results
    let (outcomes_received_count, success_count, filtered_count) =
        match aggregate_results(&args, &conn, published_count, &aggregation_pb).await {
            Ok(counts) => counts,
            Err(e) => {
                error!("Failed during result aggregation: {}", e);
                aggregation_pb.finish_with_message(format!("Aggregation failed: {}", e));
                // Optional: conn.close().await
                return Err(e);
            }
        };

    // Final Summary
    info!("--------------------");
    info!("Processing Summary:");
    info!("  Tasks Published: {}", published_count);
    //  Task Read/Serialization Errors are logged within publish_tasks
    info!("  Outcomes Received (Total): {}", outcomes_received_count);
    info!("    - Successfully Processed: {}", success_count);
    info!("    - Filtered by Worker: {}", filtered_count);
    //  Outcome Deserialization Errors are logged within aggregate_results
    info!("  (For task read/serialization and outcome deserialization errors, check logs above.)");
    info!("  Output File: {}", args.output_file);
    info!("  Excluded File: {}", args.excluded_file);
    info!("--------------------");

    // Optional: Graceful shutdown of connection can be done here if needed,
    // otherwise it will be closed on drop.
    // let _ = conn.close(200, "Producer finished successfully").await;

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
