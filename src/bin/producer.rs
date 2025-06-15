// src/bin/producer.rs

//! # Producer Binary
//!
//! This binary is responsible for the producer side of the TextBlaster pipeline.
//! Its main roles are:
//!
//! 1.  **Reading Data**: It reads text documents from a specified input Parquet file.
//!     The input file is expected to have a column for the text data and optionally
//!     an ID column.
//!
//! 2.  **Publishing Tasks**: Each text document read is serialized and published as a
//!     task to a RabbitMQ queue. These tasks are then picked up by worker instances
//!     for processing. The producer ensures tasks are published durably to survive
//!     broker restarts.
//!
//! 3.  **Aggregating Results**: After publishing tasks, the producer listens on a
//!     separate RabbitMQ queue for processing outcomes (results) from the workers.
//!     These outcomes indicate whether processing was successful, if a document was
//!     filtered, or if an error occurred.
//!
//! 4.  **Writing Output**: Based on the aggregated results, the producer writes the
//!     successfully processed documents to an output Parquet file and any documents
//!     that were filtered (e.g., due to language detection) to a separate "excluded"
//!     Parquet file.
//!
//! The producer utilizes `clap` for command-line argument parsing, `lapin` for RabbitMQ
//! interaction, `parquet` (via `TextBlaster::pipeline` modules) for reading and writing
//! Parquet files, `indicatif` for progress bars, and `tracing` for logging.
//! It also supports exposing Prometheus metrics for monitoring.

use clap::Parser;
use futures::StreamExt; // For consuming the results stream
use indicatif::{HumanDuration, ProgressBar, ProgressStyle}; // Added indicatif
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    protocol::basic::AMQPProperties,
    types::FieldTable,
};
use std::time::Instant; // Added Instant
use tracing::{error, info, warn}; // Added tracing
use tracing_subscriber::{fmt, EnvFilter}; // Added tracing_subscriber
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
/// Defines the command-line arguments accepted by the producer binary.
///
/// These arguments configure the input/output sources, RabbitMQ connection,
/// queue names, and other operational parameters.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the input Parquet file.
    /// This file should contain the text documents to be processed.
    #[arg(short, long)]
    input_file: String,

    /// Text column name in the input Parquet file.
    /// Specifies the column from which to read the text content of documents.
    #[arg(long, default_value = "text")]
    text_column: String,

    /// Optional ID column name in the input Parquet file.
    /// If provided, this column will be used to identify documents. Otherwise,
    /// IDs may be generated or handled differently by the pipeline.
    #[arg(long)]
    id_column: Option<String>,

    /// RabbitMQ connection string.
    /// Format: amqp://user:password@host:port/vhost
    /// Example: "amqp://guest:guest@localhost:5672/%2f"
    #[arg(short, long, default_value = "amqp://guest:guest@localhost:5672/%2f")]
    amqp_addr: String,

    /// Name of the RabbitMQ queue to publish tasks to.
    /// Workers will consume tasks from this queue.
    #[arg(short = 'q', long, default_value = "task_queue")]
    task_queue: String,

    /// Name of the RabbitMQ queue to consume results/outcomes from.
    /// Workers will publish processing outcomes to this queue.
    #[arg(short = 'r', long, default_value = "results_queue")]
    results_queue: String,

    /// Path to the output Parquet file for successfully processed documents.
    #[arg(short = 'o', long, default_value = "output_processed.parquet")]
    output_file: String,

    /// Path to the output Parquet file for documents that were excluded or filtered
    /// during processing (e.g., due to language detection).
    #[arg(short = 'e', long, default_value = "excluded.parquet")]
    excluded_file: String,

    /// Optional: Port for the Prometheus metrics HTTP endpoint.
    /// If specified, an HTTP server will be started on this port to expose
    /// Prometheus metrics.
    #[arg(long)]
    metrics_port: Option<u16>,
}

// --- Prometheus Metrics (now imported from TextBlaster::utils::prometheus_metrics) ---

/// Creates and configures a new `ProgressBar` or `ProgressBar::new_spinner()`
/// for displaying progress during long-running operations.
///
/// # Arguments
///
/// * `total_items` - The total number of items to process. If 0, a spinner is used,
///   suitable for when the total count is unknown.
/// * `message` - A message to display alongside the progress bar.
/// * `template` - A string defining the style and content of the progress bar.
///   See the `indicatif` crate documentation for template syntax.
///
/// # Returns
///
/// A configured `ProgressBar` instance.
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

/// Publishes tasks to a RabbitMQ queue.
///
/// This function reads documents from the input Parquet file specified in `args`,
/// serializes each document, and publishes it as a message to the RabbitMQ task queue.
/// It uses a progress bar to display publishing progress and handles durable queue
/// declaration to ensure tasks are not lost if the RabbitMQ broker restarts.
///
/// # Arguments
///
/// * `args` - A reference to the parsed command-line arguments (`Args`), containing
///   input file paths, queue names, etc.
/// * `conn` - A reference to an active `lapin::Connection` to RabbitMQ.
/// * `publishing_pb` - A reference to an `indicatif::ProgressBar` to report
///   publishing progress.
///
/// # Returns
///
/// A `Result` containing the total number of tasks successfully published (`u64`),
/// or a `PipelineError` if a fatal error occurs (e.g., failure to publish a message
/// after retries, or a channel error).
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
                match serde_json::to_vec(&doc) {
                    Ok(payload) => {
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
                            }
                            Err(e) => {
                                TASK_PUBLISH_ERRORS_TOTAL.inc();
                                publishing_pb.println(format!(
                                    "FATAL: Failed broker confirmation for task (Doc ID {}): {}. Stopping.",
                                    doc.id, e
                                ));
                                error!(
                                    doc_id = %doc.id,
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
                        publishing_pb.println(format!(
                            "Failed to serialize task (Doc ID {}): {}. Skipping.",
                            doc.id, e
                        ));
                        read_errors += 1;
                    }
                }
            }
            Err(e) => {
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
    Ok(published_count)
}

/// Aggregates processing results/outcomes from a RabbitMQ queue.
///
/// This function consumes messages from the results queue, where worker instances
/// publish the outcomes of their processing tasks. It deserializes these outcomes,
/// categorizes them (success, filtered, error), and writes the corresponding
/// `TextDocument` data to either the main output Parquet file or an excluded items
/// Parquet file.
///
/// It continues to consume messages until the number of received outcomes matches
/// the `published_count`. Progress is reported using the `aggregation_pb`.
///
/// # Arguments
///
/// * `args` - A reference to the parsed command-line arguments (`Args`).
/// * `conn` - A reference to an active `lapin::Connection` to RabbitMQ.
/// * `published_count` - The total number of tasks that were originally published.
///   This is used to determine when all expected results have been received.
/// * `aggregation_pb` - A reference to an `indicatif::ProgressBar` to report
///   aggregation progress.
///
/// # Returns
///
/// A `Result` containing a tuple `(outcomes_received_count, success_count, filtered_count)`,
/// or a `PipelineError` if a fatal error occurs (e.g., failure to initialize Parquet writers,
/// channel errors).
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
                                results_batch.push(doc);
                                if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    parquet_writer_output.write_batch(&results_batch)?;
                                    aggregation_pb.println(format!(
                                        "Written Parquet batch ({} docs). Total: {}/{}, Success: {}, Filtered: {}",
                                        results_batch.len(), outcomes_received_count, published_count, success_count, filtered_count
                                    ));
                                    results_batch.clear();
                                }
                            }
                            ProcessingOutcome::Filtered {
                                document,
                                reason: _,
                            } => {
                                filtered_count += 1;
                                RESULTS_FILTERED_TOTAL.inc();
                                excluded_batch.push(document);
                                if excluded_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    parquet_writer_excluded.write_batch(&excluded_batch)?;
                                    aggregation_pb.println(format!(
                                        "Written Filtered Parquet batch ({} docs). Total: {}/{}, Success: {}, Filtered: {}",
                                        excluded_batch.len(), outcomes_received_count, published_count, success_count, filtered_count
                                    ));
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
                        aggregation_pb.println(format!(
                            "Failed to deserialize outcome {}: {}. Payload: {:?}",
                            delivery.delivery_tag,
                            e,
                            std::str::from_utf8(&delivery.data).unwrap_or("[invalid utf8]")
                        ));
                    }
                }
                if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                    aggregation_pb.println(format!(
                        "Failed to ack outcome {}: {}. Might lead to duplicate counts.",
                        delivery.delivery_tag, ack_err
                    ));
                }
            }
            Some(Err(e)) => {
                aggregation_pb.println(format!(
                    "Error receiving outcome: {}. Will attempt to finalize current results.",
                    e
                ));
                break; // Exit loop on consumer error, then try to write remaining batches.
            }
            None => {
                aggregation_pb.println("Consumer stream closed unexpectedly. Will attempt to finalize current results.");
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

/// Main entry point for the producer binary.
///
/// Orchestrates the entire producer workflow:
/// 1. Parses command-line arguments.
/// 2. Initializes logging and optionally a Prometheus metrics endpoint.
/// 3. Connects to RabbitMQ.
/// 4. Publishes tasks from the input Parquet file to the task queue.
/// 5. If tasks were published, it then aggregates results from the results queue.
/// 6. Writes processed and excluded documents to their respective output Parquet files.
/// 7. Prints a final summary of the operations.
///
/// # Returns
///
/// `Result<()>` which is `Ok(())` on successful completion of all tasks,
/// or a `PipelineError` if any stage of the process encounters an unrecoverable error.
#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize tracing subscriber
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set
    fmt::Subscriber::builder().with_env_filter(filter).init();

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
