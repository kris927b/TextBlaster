use clap::Parser;
use futures::StreamExt; // For processing the consumer stream
use rust_data::data_model::TextDocument;
use rust_data::error::{PipelineError, Result}; // Use the library's Result type
use rust_data::executor::{PipelineExecutor, ProcessingStep};
// Import necessary filters (adjust if steps change)
use arrow::datatypes::ArrowNativeType;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties, Result as LapinResult,
};
use rust_data::pipeline::filters::{C4QualityFilter, GopherQualityFilter, GopherRepetitionFilter};
use rust_data::utils::text::DANISH_STOP_WORDS; // If GopherQualityFilter uses it
use std::sync::Arc; // To share the executor across potential concurrent tasks
use std::time::Duration;
use tokio::time::sleep; // Needed for as_usize()

// Define command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// RabbitMQ connection string (e.g., amqp://guest:guest@localhost:5672/%2f)
    #[arg(short, long, default_value = "amqp://guest:guest@localhost:5672/%2f")]
    amqp_addr: String,

    /// Name of the queue to consume tasks from
    #[arg(short, long, default_value = "task_queue")]
    queue_name: String,

    /// Prefetch count (how many messages to buffer locally)
    #[arg(long, default_value_t = 10)] // Adjust based on task duration/resources
    prefetch_count: u16,
}

// Re-use the connection helper from producer (or move to lib.rs if desired)
async fn connect_rabbitmq(addr: &str) -> LapinResult<Connection> {
    let options = ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);
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
                    return Err(e);
                }
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// Function to build the pipeline (similar to old main.rs)
// Consider moving pipeline construction logic to config.rs or a dedicated builder later
fn build_pipeline() -> Vec<Box<dyn ProcessingStep>> {
    vec![
        Box::new(C4QualityFilter::new(
            10.as_usize(), // Example values
            3.as_usize(),
            100.as_usize(),
        )),
        Box::new(GopherRepetitionFilter::new(
            Some(0.3),
            Some(0.3),
            Some(0.2),
            Some(0.2),
            vec![(2, 0.2), (3, 0.18), (4, 0.16)],
            vec![
                (5, 0.15),
                (6, 0.14),
                (7, 0.13),
                (8, 0.12),
                (9, 0.11),
                (10, 0.10),
            ],
        )),
        Box::new(GopherQualityFilter::new(
            Some(50),
            Some(1000000),
            Some(3.0),
            Some(10.0),
            Some(0.1),
            Some(0.9),
            Some(0.3),
            Some(0.8),
            Some(2),
            Some(DANISH_STOP_WORDS.iter().map(|s| s.to_string()).collect()),
        )),
        // Add other steps as needed
    ]
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Worker starting.");
    println!(
        "Consuming from queue '{}' @ {}",
        args.queue_name, args.amqp_addr
    );
    println!("Prefetch count: {}", args.prefetch_count);

    // 1. Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr)
        .await
        .map_err(|e| PipelineError::QueueError(format!("Worker failed to connect: {}", e)))?;

    let channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!("Worker failed to create channel: {}", e))
    })?;

    // 2. Declare the same queue (ensure durability matches producer)
    let _queue = channel // Use _queue as we only need to ensure it exists
        .queue_declare(
            &args.queue_name,
            QueueDeclareOptions {
                durable: true, // MUST match the producer's declaration
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .map_err(|e| PipelineError::QueueError(format!("Worker failed to declare queue: {}", e)))?;

    // 3. Set Quality of Service (Prefetch Count)
    // This tells RabbitMQ how many messages this worker can handle concurrently
    // before needing acknowledgements. Prevents overwhelming the worker.
    channel
        .basic_qos(args.prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to set QoS: {}", e)))?;

    // 4. Create the Pipeline Executor (potentially shareable)
    let pipeline_steps = build_pipeline();
    if pipeline_steps.is_empty() {
        println!("Warning: Starting worker with an empty pipeline!");
    }
    let executor = Arc::new(PipelineExecutor::new(pipeline_steps)); // Arc allows sharing if needed

    // 5. Start Consuming Messages
    let consumer_tag = format!(
        "worker-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp()
    ); // Unique consumer tag
    let mut consumer = channel
        .basic_consume(
            &args.queue_name,
            &consumer_tag,                  // Unique identifier for this consumer
            BasicConsumeOptions::default(), // Auto-ack is false by default, which is good
            FieldTable::default(),
        )
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to start consuming: {}", e)))?;

    println!("Worker started consuming. Waiting for messages...");

    // 6. Process messages from the stream
    // The consumer object is a stream of Deliveries
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                // Got a message
                let executor_clone = Arc::clone(&executor); // Clone Arc for the async task
                                                            // Spawn a Tokio task to process the message concurrently
                                                            // This allows processing multiple messages up to the prefetch limit
                tokio::spawn(async move {
                    // Deserialize
                    match serde_json::from_slice::<TextDocument>(&delivery.data) {
                        Ok(doc) => {
                            println!("Processing document ID: {}", doc.id);
                            // Run the pipeline
                            match executor_clone.run_single_async(doc).await {
                                Ok(processed_doc) => {
                                    println!(
                                        "Successfully processed document ID: {}",
                                        processed_doc.id
                                    );
                                    // Acknowledge the message
                                    if let Err(ack_err) =
                                        delivery.ack(BasicAckOptions::default()).await
                                    {
                                        eprintln!(
                                            "Failed to ack message {}: {}",
                                            delivery.delivery_tag, ack_err
                                        );
                                        // Potential issue: message might be redelivered if ack fails consistently
                                    }
                                }
                                Err(e) => {
                                    // Processing failed (e.g., filtered out, step error)
                                    eprintln!(
                                        "Pipeline error processing doc ID {}: {}",
                                        delivery.delivery_tag, e
                                    );
                                    // Decide how to handle: nack/reject? dead-letter queue? For now, just ack as processed (but failed).
                                    // Acknowledging prevents infinite retries for documents that will always fail processing.
                                    if let Err(ack_err) =
                                        delivery.ack(BasicAckOptions::default()).await
                                    {
                                        eprintln!(
                                            "Failed to ack message {} after processing error: {}",
                                            delivery.delivery_tag, ack_err
                                        );
                                    }
                                    // Alternative: Nack without requeue (moves to dead-letter queue if configured)
                                    // channel_ref.basic_nack(delivery.delivery_tag, BasicNackOptions { multiple: false, requeue: false }).await;
                                }
                            }
                        }
                        Err(e) => {
                            // Deserialization failed - message is likely corrupt
                            eprintln!(
                                "Failed to deserialize message {}: {}. Payload: {:?}",
                                delivery.delivery_tag,
                                e,
                                std::str::from_utf8(&delivery.data).unwrap_or("[invalid utf8]")
                            );
                            // Acknowledge the poison pill message to remove it from the queue
                            if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                                eprintln!(
                                    "Failed to ack message {} after deserialization error: {}",
                                    delivery.delivery_tag, ack_err
                                );
                            }
                        }
                    }
                });
            }
            Err(e) => {
                // Error receiving message from the stream (e.g., channel closed)
                eprintln!("Error receiving message from consumer stream: {}", e);
                // Attempt to reconnect or terminate? For simplicity, break the loop.
                break;
            }
        }
    }

    println!("Worker stopped consuming.");
    // Optional: Close channel/connection gracefully
    // channel.close(200, "Worker finished").await?;
    // conn.close(200, "Worker finished").await?;

    Ok(()) // Should ideally not be reached unless consumption stops
}
