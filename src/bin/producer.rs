use clap::Parser;
use rust_data::config::ParquetInputConfig;
// use rust_data::data_model::TextDocument; // Removed as it's inferred from reader/serialization
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    protocol::basic::AMQPProperties,
    Connection, ConnectionProperties, Result as LapinResult,
};
use rust_data::error::Result; // Use the library's Result type
use rust_data::pipeline::readers::ParquetReader;
use std::time::Duration;
use tokio::time::sleep;

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
    #[arg(short, long, default_value = "task_queue")]
    queue_name: String,
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
    println!("Queue: {} @ {}", args.queue_name, args.amqp_addr);

    // 1. Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr).await?;
    let channel = conn.create_channel().await?;

    // 2. Declare the queue (idempotent)
    let queue = channel
        .queue_declare(
            &args.queue_name,
            QueueDeclareOptions {
                durable: true, // Make queue durable (survive broker restart)
                ..Default::default()
            },
            Default::default(),
        )
        .await?;
    println!("Declared queue '{}'", queue.name().as_str());

    // 3. Configure and create the Parquet Reader
    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone(),
        batch_size: Some(1024), // Process in reasonable batches if iterator supports it
    };
    let reader = ParquetReader::new(parquet_config);

    // 4. Read documents and publish to the queue
    println!("Reading documents and publishing to queue...");
    let mut published_count = 0;
    let mut read_errors = 0;

    // Get the iterator from the reader
    let doc_iterator = reader.read_documents()?; // This returns an iterator of Result<TextDocument>

    for doc_result in doc_iterator {
        match doc_result {
            Ok(doc) => {
                // Serialize the document to JSON bytes
                match serde_json::to_vec(&doc) {
                    Ok(payload) => {
                        // Publish the payload to the queue
                        let publish_result = channel
                            .basic_publish(
                                "", // Default exchange
                                &args.queue_name,
                                BasicPublishOptions::default(),
                                &payload,
                                AMQPProperties::default().with_delivery_mode(2), // Make message persistent
                            )
                            .await? // Propagate lapin errors
                            .await; // Wait for confirmation from broker

                        match publish_result {
                            Ok(_) => {
                                published_count += 1;
                                if published_count % 100 == 0 {
                                    // Log progress periodically
                                    println!("Published {} documents...", published_count);
                                }
                            }
                            Err(e) => {
                                // This error is less common but means broker rejected publish
                                eprintln!(
                                    "Failed to get broker confirmation for doc ID {}: {}",
                                    doc.id, e
                                );
                                // Decide how to handle: retry? log and skip? terminate?
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to serialize document ID {}: {}", doc.id, e);
                        // Skip this document
                        read_errors += 1; // Count serialization errors as read errors for simplicity
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading document: {}", e);
                read_errors += 1;
                // Skip this document
            }
        }
    }

    // 5. Close connection (optional, happens on drop)
    // channel.close(200, "OK").await?;
    // conn.close(200, "OK").await?;

    println!(
        "Producer finished. Published: {}, Read/Serialization Errors: {}",
        published_count, read_errors
    );

    Ok(())
}
