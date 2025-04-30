// src/bin/worker.rs

use clap::Parser;
use futures::StreamExt; // For processing the consumer stream
use rust_data::data_model::{ProcessingOutcome, TextDocument}; // Updated import
use rust_data::error::{PipelineError, Result}; // Use the library's Result type
use rust_data::executor::{PipelineExecutor, ProcessingStep};
// Import necessary filters (adjust if steps change)
use arrow::datatypes::ArrowNativeType; // Needed for as_usize() in build_pipeline
use lapin::{
    options::{
        BasicAckOptions,
        BasicConsumeOptions,
        BasicPublishOptions,
        BasicQosOptions, // Added BasicPublishOptions
        QueueDeclareOptions,
    },
    protocol::basic::AMQPProperties, // Added AMQPProperties
    types::FieldTable,
    Connection,
    ConnectionProperties,
    Result as LapinResult,
};
use rust_data::pipeline::filters::{C4QualityFilter, GopherQualityFilter, GopherRepetitionFilter};
use rust_data::utils::text::DANISH_STOP_WORDS; // If GopherQualityFilter uses it
use serde_json;
use std::sync::Arc; // To share the executor across potential concurrent tasks
use std::time::Duration;
use tokio::time::sleep; // {{ Add serde_json for result serialization }}

// Define command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// RabbitMQ connection string (e.g., amqp://guest:guest@localhost:5672/%2f)
    #[arg(short, long, default_value = "amqp://guest:guest@localhost:5672/%2f")]
    amqp_addr: String,

    /// Name of the queue to consume tasks from
    #[arg(short = 'q', long, default_value = "task_queue")]
    // Use short arg 'q' consistent with producer
    task_queue: String,

    /// Name of the queue to publish results/outcomes to
    #[arg(short = 'r', long, default_value = "results_queue")]
    // Use short arg 'r' consistent with producer
    results_queue: String,

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
        "Consuming from queue '{}', publishing outcomes to '{}' @ {}", // Updated log
        args.task_queue, args.results_queue, args.amqp_addr
    );
    println!("Prefetch count: {}", args.prefetch_count);

    // 1. Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr)
        .await
        .map_err(|e| PipelineError::QueueError(format!("Worker failed to connect: {}", e)))?;

    // Create two channels: one for consuming tasks, one for publishing results/outcomes
    // This avoids potential channel-level blocking issues if one operation stalls.
    let consume_channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!("Worker failed to create consume channel: {}", e))
    })?;
    let publish_channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!("Worker failed to create publish channel: {}", e))
    })?;

    // 2. Declare the task queue (ensure durability matches producer)
    let _task_queue = consume_channel
        .queue_declare(
            &args.task_queue,
            QueueDeclareOptions {
                durable: true, // MUST match the producer's declaration
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .map_err(|e| {
            PipelineError::QueueError(format!("Worker failed to declare task queue: {}", e))
        })?;

    // 2b. Declare the results queue (also durable)
    let _results_queue = publish_channel
        .queue_declare(
            &args.results_queue,
            QueueDeclareOptions {
                durable: true, // Results/Outcomes should also survive restarts
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .map_err(|e| {
            PipelineError::QueueError(format!("Worker failed to declare results queue: {}", e))
        })?;

    // 3. Set Quality of Service (Prefetch Count) on the consume channel
    consume_channel
        .basic_qos(args.prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to set QoS: {}", e)))?;

    // 4. Create the Pipeline Executor
    let pipeline_steps = build_pipeline();
    if pipeline_steps.is_empty() {
        println!("Warning: Starting worker with an empty pipeline!");
    }
    let executor = Arc::new(PipelineExecutor::new(pipeline_steps));

    // 5. Start Consuming Messages from the task queue
    let consumer_tag = format!(
        "worker-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp()
    );
    let mut consumer = consume_channel
        .basic_consume(
            &args.task_queue,
            &consumer_tag,
            BasicConsumeOptions::default(), // auto_ack: false (default)
            FieldTable::default(),
        )
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to start consuming: {}", e)))?;

    println!("Worker started consuming tasks. Waiting for messages...");

    // 6. Process messages from the stream
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let executor_clone = Arc::clone(&executor);
                let publish_channel_clone = publish_channel.clone();
                let results_queue_name = args.results_queue.clone();

                // Spawn a Tokio task to process the message concurrently
                tokio::spawn(async move {
                    match serde_json::from_slice::<TextDocument>(&delivery.data) {
                        Ok(doc) => {
                            let outcome: Option<ProcessingOutcome>; // Variable to hold the outcome message
                            let original_doc_id = doc.id.clone(); // Clone id for logging in case of error/filtering

                            println!("Processing document ID: {}", original_doc_id);
                            match executor_clone.run_single_async(doc).await {
                                // Success case remains the same
                                Ok(processed_doc) => {
                                    println!(
                                        "Successfully processed document ID: {}",
                                        processed_doc.id
                                    );
                                    // Set outcome to Success
                                    outcome = Some(ProcessingOutcome::Success(processed_doc));
                                }

                                Err(pipeline_error) => {
                                    // Check if the top-level error is a StepError
                                    if let PipelineError::StepError { step_name, source } =
                                        pipeline_error
                                    {
                                        // Now, check the *source* of the StepError
                                        // We need to dereference the Box to match the inner error
                                        match *source {
                                            PipelineError::DocumentFiltered { doc_id, reason } => {
                                                // Found the filtered error inside StepError!
                                                println!(
                                                    "Document ID: {} was filtered by step '{}'. Reason: {}",
                                                    doc_id, step_name, reason
                                                );
                                                // Set outcome to Filtered
                                                outcome = Some(ProcessingOutcome::Filtered {
                                                    id: doc_id,
                                                    reason,
                                                });
                                            }
                                            // Any other error nested within StepError is a genuine processing error
                                            other_error => {
                                                eprintln!(
                                                    "Pipeline step '{}' failed for doc ID {}: {}",
                                                    step_name,
                                                    original_doc_id,
                                                    other_error // Log the inner error
                                                );
                                                outcome = None; // Don't send an outcome for pipeline errors
                                            }
                                        }
                                    } else {
                                        // Handle errors returned by run_single_async that *aren't* StepErrors
                                        // (Currently unlikely based on executor code, but good practice)
                                        eprintln!(
                                            "Unexpected pipeline error for doc ID {}: {}",
                                            original_doc_id, pipeline_error
                                        );
                                        outcome = None; // Don't send an outcome
                                    }
                                }
                            }

                            // The rest of the code (publishing outcome, acking) remains the same...
                            // Send outcome back to producer if it's Success or Filtered
                            if let Some(ref actual_outcome) = outcome {
                                match serde_json::to_vec(actual_outcome) {
                                    Ok(payload) => {
                                        // ... (publish logic)
                                        let publish_confirm = publish_channel_clone
                                            .basic_publish(
                                                "", // Default exchange
                                                &results_queue_name,
                                                BasicPublishOptions::default(),
                                                &payload,
                                                AMQPProperties::default().with_delivery_mode(2), // Persistent
                                            )
                                            .await;

                                        match publish_confirm {
                                            Ok(confirmation) => match confirmation.await { // Wait for broker confirmation
                                                Ok(_) => println!("Published outcome for doc ID: {}", original_doc_id), // Use original_doc_id
                                                Err(e) => eprintln!("Failed publish confirmation for outcome (Doc ID {}): {}", original_doc_id, e),
                                            },
                                            Err(e) => eprintln!("Failed to initiate publish for outcome (Doc ID {}): {}", original_doc_id, e),
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "Failed to serialize outcome for doc ID {}: {}",
                                            original_doc_id, e
                                        );
                                    }
                                }
                            }

                            // Acknowledge the original task message regardless of outcome
                            if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                                eprintln!(
                                    "Failed to ack task message {} for doc ID {}: {}",
                                    delivery.delivery_tag, original_doc_id, ack_err
                                );
                            }
                        }
                        Err(e) => {
                            // Handle deserialization errors
                            eprintln!(
                                "Failed to deserialize task message {}: {}. Payload: {:?}",
                                delivery.delivery_tag,
                                e,
                                std::str::from_utf8(&delivery.data).unwrap_or("[invalid utf8]")
                            );
                            // Acknowledge the poison pill task message
                            if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                                eprintln!(
                                    "Failed to ack task message {} after deserialization error: {}",
                                    delivery.delivery_tag, ack_err
                                );
                            }
                        }
                    }
                });
            }
            Err(e) => {
                // Handle errors in receiving messages from RabbitMQ (e.g., channel closed)
                eprintln!("Error receiving task message from consumer stream: {}", e);
                // Break the loop to stop the worker if the consumer stream fails
                break;
            }
        }
    }

    println!("Worker stopped consuming tasks.");

    // Optional: Graceful shutdown of channels and connection
    // Note: Depending on error handling, channels might already be closed.
    // Use `close` method with appropriate code and reason.
    // let _ = consume_channel.close(200, "Worker shutting down normally").await;
    // let _ = publish_channel.close(200, "Worker shutting down normally").await;
    // let _ = conn.close(200, "Worker shutting down normally").await;

    Ok(())
}
