// src/bin/producer.rs

use clap::Parser;
use futures::StreamExt; // For consuming the results stream
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    protocol::basic::AMQPProperties,
    types::FieldTable,
    Connection, ConnectionProperties, Result as LapinResult,
};
use serde_json;
use std::time::Duration;
use tokio::time::sleep;
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
                println!("Successfully connected to RabbitMQ at {}", addr);
                return Ok(conn);
            }
            Err(e) => {
                attempts += 1;
                eprintln!(
                    "Failed to connect to RabbitMQ (attempt {}): {}. Retrying in 5 seconds...",
                    attempts, e
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

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    println!("Producer started.");
    println!("Input file: {}", args.input_file);
    println!("Task Queue: {} @ {}", args.task_queue, args.amqp_addr);
    println!("Results/Outcomes Queue: {}", args.results_queue);
    println!("Output File: {}", args.output_file);

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
    println!("Declared durable task queue '{}'", args.task_queue);

    // 3. Configure and create the Parquet Reader
    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone(),
        batch_size: Some(1024), // Example batch size for reading
    };
    let reader = ParquetReader::new(parquet_config);

    // 4. Read documents and publish to the task queue
    println!("Reading documents and publishing tasks...");
    let mut published_count = 0u64; // Use u64 for potentially large counts
    let mut read_errors = 0u64;
    let doc_iterator = reader.read_documents()?;

    for doc_result in doc_iterator {
        match doc_result {
            Ok(doc) => {
                match serde_json::to_vec(&doc) {
                    Ok(payload) => {
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

                        match publish_confirm {
                            Ok(_) => {
                                published_count += 1;
                                if published_count % 1000 == 0 {
                                    // Log progress periodically
                                    println!("Published {} tasks...", published_count);
                                }
                            }
                            Err(e) => {
                                // If broker confirmation fails, this is serious.
                                // Maybe stop, retry, or just log and potentially lose the task.
                                eprintln!(
                                    "FATAL: Failed broker confirmation for task (Doc ID {}): {}. Stopping.",
                                    doc.id, e
                                );
                                // Decide if this error should stop the producer
                                return Err(PipelineError::QueueError(format!(
                                    "Publish confirmation failed: {}",
                                    e
                                )));
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to serialize task (Doc ID {}): {}. Skipping.",
                            doc.id, e
                        );
                        read_errors += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading document for task: {}. Skipping.", e);
                read_errors += 1;
            }
        }
    }

    println!(
        "Producer finished publishing tasks. Total Published: {}, Read/Serialization Errors: {}",
        published_count, read_errors
    );

    // Early exit if no tasks were published (nothing to wait for)
    if published_count == 0 {
        println!("No tasks were published. Exiting.");
        // Close channel/connection before exiting
        // let _ = publish_channel.close(200, "No tasks published").await;
        // let _ = conn.close(200, "No tasks published").await;
        return Ok(());
    }

    // --- Phase 2: Consume results/outcomes and write to Parquet ---
    println!("Starting results aggregation phase...");

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
    println!("Declared durable results queue '{}'", args.results_queue);

    // 6. Initialize Parquet Writer
    // Ensure the directory for the output file exists
    if let Some(parent_dir) = std::path::Path::new(&args.output_file).parent() {
        tokio::fs::create_dir_all(parent_dir).await?; // Create parent dirs if needed
    }
    let mut parquet_writer_output = ParquetWriter::new(&args.output_file)?;
    println!("Initialized Parquet writer for: {}", args.output_file);

    let mut parquet_writer_excluded = ParquetWriter::new(&args.excluded_file)?;
    println!("Initialized Parquet writer for: {}", args.excluded_file);

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

    println!(
        "Waiting for outcomes from queue '{}'. Expecting {} outcomes (Success or Filtered).",
        args.results_queue, published_count
    );

    // Loop until we have received an outcome for every task published
    while outcomes_received_count < published_count {
        let delivery_result = consumer.next().await;

        if delivery_result.is_none() {
            // Stream closed unexpectedly (e.g., connection lost)
            eprintln!("ERROR: Consumer stream closed unexpectedly before receiving all outcomes.");
            break; // Exit the loop
        }

        match delivery_result.unwrap() {
            // We checked for None above
            Ok(delivery) => {
                // Deserialize the received outcome message
                match serde_json::from_slice::<ProcessingOutcome>(&delivery.data) {
                    Ok(outcome) => {
                        outcomes_received_count += 1; // Increment for ANY valid outcome

                        match outcome {
                            ProcessingOutcome::Success(doc) => {
                                success_count += 1;
                                results_batch.push(doc);
                                // Write batch if full
                                if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    match parquet_writer_output.write_batch(&results_batch) {
                                        Ok(_) => {
                                            println!(
                                                "Written Parquet batch ({} docs). Total outcomes: {}/{}, Success: {}, Filtered: {}",
                                                results_batch.len(), outcomes_received_count, published_count, success_count, filtered_count
                                            );
                                            results_batch.clear(); // Clear batch after successful write
                                        }
                                        Err(e) => {
                                            // This is a critical error during writing
                                            eprintln!("FATAL: Failed to write Parquet batch: {}. Stopping.", e);
                                            // Ack the current message first before stopping
                                            let _ = delivery.ack(BasicAckOptions::default()).await;
                                            break; // Stop processing further outcomes
                                        }
                                    }
                                }
                            }
                            ProcessingOutcome::Filtered {
                                document,
                                reason: _,
                            } => {
                                filtered_count += 1;
                                excluded_batch.push(document);
                                // Write batch if full
                                if excluded_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                                    match parquet_writer_excluded.write_batch(&excluded_batch) {
                                        Ok(_) => {
                                            println!(
                                                "Written Filtered Parquet batch ({} docs). Total outcomes: {}/{}, Success: {}, Filtered: {}",
                                                excluded_batch.len(), outcomes_received_count, published_count, success_count, filtered_count
                                            );
                                            excluded_batch.clear(); // Clear batch after successful write
                                        }
                                        Err(e) => {
                                            // This is a critical error during writing
                                            eprintln!("FATAL: Failed to write Parquet batch: {}. Stopping.", e);
                                            // Ack the current message first before stopping
                                            let _ = delivery.ack(BasicAckOptions::default()).await;
                                            break; // Stop processing further outcomes
                                        }
                                    }
                                }
                            } // Handle ProcessingOutcome::Error here if added later
                        }
                    }
                    Err(e) => {
                        // Handle malformed outcome messages (poison pills)
                        outcome_deserialization_errors += 1;
                        eprintln!(
                            "Failed to deserialize outcome message {}: {}. Payload: {:?}",
                            delivery.delivery_tag,
                            e,
                            std::str::from_utf8(&delivery.data).unwrap_or("[invalid utf8]")
                        );
                        // Acknowledge the poison pill outcome to remove it from queue
                    }
                }

                // Acknowledge the outcome message *after* processing it
                if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                    eprintln!(
                        "Failed to ack outcome message {}: {}. This might lead to duplicate processing counts.",
                        delivery.delivery_tag, ack_err
                    );
                    // Consider how to handle persistent ack failures. Maybe stop?
                }
            }
            Err(e) => {
                // Handle errors in receiving messages from RabbitMQ (e.g., channel closed)
                eprintln!(
                    "Error receiving outcome message from consumer stream: {}",
                    e
                );
                break; // Stop consuming if the stream breaks
            }
        }
    }

    println!(
        "Finished consuming outcomes (Received {}/{} expected).",
        outcomes_received_count, published_count
    );

    // 8. Write any remaining documents in the final batch
    if !results_batch.is_empty() {
        println!(
            "Writing final batch of {} successfully processed documents...",
            results_batch.len()
        );
        match parquet_writer_output.write_batch(&results_batch) {
            Ok(_) => {
                println!("Final batch written successfully.");
            }
            Err(e) => {
                eprintln!("ERROR: Failed to write final Parquet batch: {}", e);
                // File might be incomplete/corrupted here
            }
        }
    }

    // 8. Write any remaining documents in the final batch
    if !excluded_batch.is_empty() {
        println!(
            "Writing final batch of {} successfully processed documents...",
            excluded_batch.len()
        );
        match parquet_writer_excluded.write_batch(&excluded_batch) {
            Ok(_) => {
                println!("Final batch written successfully.");
            }
            Err(e) => {
                eprintln!("ERROR: Failed to write final Parquet batch: {}", e);
                // File might be incomplete/corrupted here
            }
        }
    }

    // 9. Close the Parquet writer (essential!)
    println!("Closing Parquet writer...");
    if let Err(e) = parquet_writer_output.close() {
        eprintln!("ERROR: Failed to close Parquet writer cleanly: {}", e);
    } else {
        println!("Parquet writer closed successfully.");
    }

    if let Err(e) = parquet_writer_excluded.close() {
        eprintln!("ERROR: Failed to close Parquet writer cleanly: {}", e);
    } else {
        println!("Parquet writer closed successfully.");
    }

    // 10. Final Summary
    println!("--------------------");
    println!("Processing Summary:");
    println!("  Tasks Published: {}", published_count);
    println!("  Task Read/Serialization Errors: {}", read_errors);
    println!("  Outcomes Received (Total): {}", outcomes_received_count);
    println!("    - Successfully Processed: {}", success_count);
    println!("    - Filtered by Worker: {}", filtered_count);
    println!(
        "  Outcome Deserialization Errors: {}",
        outcome_deserialization_errors
    );
    println!("  Output File: {}", args.output_file);
    println!("--------------------");

    // Optional: Graceful shutdown of channels and connection
    // let _ = consume_channel.close(200, "Producer finished aggregation").await;
    // let _ = publish_channel.close(200, "Producer finished publishing").await;
    // let _ = conn.close(200, "Producer finished").await;

    // Final check for mismatch
    if outcomes_received_count != published_count {
        eprintln!(
            "Warning: Number of outcomes received ({}) does not match tasks published ({}). Output might be incomplete or counts inaccurate due to errors or message loss.",
            outcomes_received_count, published_count
        );
    }

    Ok(())
}
