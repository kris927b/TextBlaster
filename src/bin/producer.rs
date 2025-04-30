use clap::Parser;
use futures::StreamExt; // {{ Add StreamExt for consumer }}
use lapin::{
    options::{
        BasicAckOptions,
        BasicConsumeOptions,
        BasicPublishOptions,
        QueueDeclareOptions, // {{ Added Ack/Consume }}
    },
    protocol::basic::AMQPProperties,
    types::FieldTable, // {{ Added FieldTable }}
    Connection,
    ConnectionProperties,
    Result as LapinResult,
};
use rust_data::config::ParquetInputConfig;
use rust_data::data_model::TextDocument; // {{ Add TextDocument import }}
use rust_data::error::Result;
use rust_data::pipeline::readers::ParquetReader;
use rust_data::pipeline::writers::parquet_writer::ParquetWriter; // {{ Add ParquetWriter import }}
use serde_json;
use std::time::Duration;
use tokio::time::sleep; // {{ Add serde_json for deserializing results }}

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
    #[arg(short = 'q', long, default_value = "task_queue")] // {{ Short arg 'q' }}
    task_queue: String, // {{ Renamed from queue_name }}

    /// Name of the queue to consume results from
    #[arg(short = 'r', long, default_value = "results_queue")] // {{ Added results queue arg }}
    results_queue: String,

    /// Path to the output Parquet file
    #[arg(short = 'o', long, default_value = "output_processed.parquet")]
    // {{ Added output file arg }}
    output_file: String,
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
    println!("Results Queue: {}", args.results_queue);
    println!("Output File: {}", args.output_file);

    // 1. Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr).await?;
    let publish_channel = conn.create_channel().await?; // Channel for publishing tasks

    // 2. Declare the task queue
    let _task_queue_info = publish_channel // {{ Use clearer variable name }}
        .queue_declare(
            &args.task_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await?;
    // println!("Declared task queue '{}'", task_queue_info.name().as_str()); // Redundant if name matches

    // 3. Configure and create the Parquet Reader
    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone(),
        batch_size: Some(1024),
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
                        let publish_confirm = publish_channel // {{ Use publish_channel }}
                            .basic_publish(
                                "", // Default exchange
                                &args.task_queue,
                                BasicPublishOptions::default(),
                                &payload,
                                AMQPProperties::default().with_delivery_mode(2),
                            )
                            .await?
                            .await;

                        match publish_confirm {
                            Ok(_) => {
                                published_count += 1;
                                if published_count % 1000 == 0 {
                                    // Log less frequently
                                    println!("Published {} tasks...", published_count);
                                }
                            }
                            Err(e) => {
                                // Consider if this should be fatal or just logged
                                eprintln!(
                                    "Failed broker confirmation for task (Doc ID {}): {}",
                                    doc.id, e
                                );
                                // Maybe increment an error count here?
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to serialize task (Doc ID {}): {}", doc.id, e);
                        read_errors += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading document for task: {}", e);
                read_errors += 1;
            }
        }
    }

    println!(
        "Producer finished publishing tasks. Published: {}, Read/Serialization Errors: {}",
        published_count, read_errors
    );

    if published_count == 0 {
        println!("No tasks were published. Exiting.");
        // Close channel/connection before exiting early
        // publish_channel.close(200, "No tasks published").await?;
        // conn.close(200, "No tasks published").await?;
        return Ok(());
    }

    // --- Phase 2: Consume results and write to Parquet ---
    println!("Starting results aggregation phase...");

    // 5. Create Channel and Declare Results Queue
    let consume_channel = conn.create_channel().await?; // New channel for consuming results
    let _results_queue_info = consume_channel
        .queue_declare(
            &args.results_queue,
            QueueDeclareOptions {
                durable: true, // Should match worker's declaration
                ..Default::default()
            },
            Default::default(),
        )
        .await?;
    // println!("Declared results queue '{}'", results_queue_info.name().as_str());

    // 6. Initialize Parquet Writer
    // Ensure the directory for the output file exists (optional but good practice)
    if let Some(parent_dir) = std::path::Path::new(&args.output_file).parent() {
        tokio::fs::create_dir_all(parent_dir).await?; // Create parent directories if needed
    }
    let mut parquet_writer = ParquetWriter::new(&args.output_file)?;
    println!("Initialized Parquet writer for: {}", args.output_file);

    // 7. Start Consuming Results
    let mut results_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut results_received_count = 0u64;
    let mut result_deserialization_errors = 0u64;

    let consumer_tag = format!(
        "producer-aggregator-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp()
    );
    let mut consumer = consume_channel
        .basic_consume(
            &args.results_queue,
            &consumer_tag,
            BasicConsumeOptions::default(), // Auto-ack false is default
            FieldTable::default(),
        )
        .await?;

    println!(
        "Waiting for results from queue '{}'. Expecting {} results.",
        args.results_queue, published_count
    );

    while let Some(delivery_result) = consumer.next().await {
        if results_received_count >= published_count {
            // We already received all expected messages, but more might be coming?
            // This might indicate an issue (e.g., duplicate processing).
            // For now, we'll log and break. Consider rejecting unexpected messages.
            eprintln!(
                "Warning: Received more results ({}) than tasks published ({}). Stopping consumption.",
                results_received_count, published_count
            );
            // Ideally, reject the unexpected message before breaking
            // if let Ok(delivery) = delivery_result {
            //    let _ = delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await;
            // }
            break;
        }

        match delivery_result {
            Ok(delivery) => {
                // Deserialize result
                match serde_json::from_slice::<TextDocument>(&delivery.data) {
                    Ok(doc) => {
                        results_batch.push(doc);
                        results_received_count += 1;

                        // Write batch if full
                        if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                            match parquet_writer.write_batch(&results_batch) {
                                Ok(_) => {
                                    println!(
                                        "Written Parquet batch. Total results processed: {}/{}",
                                        results_received_count, published_count
                                    );
                                    results_batch.clear(); // Clear batch after successful write
                                }
                                Err(e) => {
                                    // Fatal error? Or log and continue? Depends on requirements.
                                    eprintln!("FATAL: Failed to write Parquet batch: {}", e);
                                    // Decide how to proceed: maybe stop consuming, maybe try later?
                                    // For now, stop processing.
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        result_deserialization_errors += 1;
                        eprintln!(
                            "Failed to deserialize result message {}: {}. Payload: {:?}",
                            delivery.delivery_tag,
                            e,
                            std::str::from_utf8(&delivery.data).unwrap_or("[invalid utf8]")
                        );
                        // Acknowledge the poison pill result to remove it
                    }
                }

                // Acknowledge the result message
                if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                    eprintln!(
                        "Failed to ack result message {}: {}",
                        delivery.delivery_tag, ack_err
                    );
                    // This could lead to duplicate processing if the worker resends.
                    // Consider how to handle persistent ack failures.
                }

                // Check if all expected results have been processed (after acking)
                if results_received_count == published_count {
                    println!("All expected results received ({})", published_count);
                    break; // Exit the consumption loop
                }
            }
            Err(e) => {
                eprintln!("Error receiving result message from consumer stream: {}", e);
                break; // Stop consuming if the stream breaks
            }
        }
    }

    println!("Finished consuming results.");

    // 8. Write any remaining documents in the final batch
    if !results_batch.is_empty() {
        println!(
            "Writing final batch of {} documents...",
            results_batch.len()
        );
        match parquet_writer.write_batch(&results_batch) {
            Ok(_) => {
                println!("Final batch written successfully.");
            }
            Err(e) => {
                eprintln!("ERROR: Failed to write final Parquet batch: {}", e);
                // File might be incomplete/corrupted here
            }
        }
    }

    // 9. Close the Parquet writer
    println!("Closing Parquet writer...");
    if let Err(e) = parquet_writer.close() {
        eprintln!("ERROR: Failed to close Parquet writer cleanly: {}", e);
    } else {
        println!("Parquet writer closed successfully.");
    }

    // 10. Final Summary
    println!("--------------------");
    println!("Processing Summary:");
    println!("  Tasks Published: {}", published_count);
    println!("  Read/Serialization Errors (Producer): {}", read_errors);
    println!("  Results Received: {}", results_received_count);
    println!(
        "  Result Deserialization Errors (Producer): {}",
        result_deserialization_errors
    );
    println!("  Output File: {}", args.output_file);
    println!("--------------------");

    // Optional: Close channels/connection gracefully
    // consume_channel.close(200, "Producer finished aggregation").await?;
    // publish_channel.close(200, "Producer finished").await?; // Ensure publish channel is closed too
    // conn.close(200, "Producer finished").await?;

    if results_received_count != published_count {
        eprintln!("Warning: Number of results received ({}) does not match tasks published ({}). Output might be incomplete.", results_received_count, published_count);
    }

    Ok(())
}
