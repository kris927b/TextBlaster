// src/bin/worker.rs

use clap::Parser;
use futures::StreamExt; // For processing the consumer stream
                        // {{ Use the new load_pipeline_config function }}
use TextBlaster::config::{load_pipeline_config, PipelineConfig, StepConfig}; // Added config imports and load_pipeline_config
use TextBlaster::data_model::{ProcessingOutcome, TextDocument}; // Updated import
use TextBlaster::error::{PipelineError, Result}; // Use the library's Result type
use TextBlaster::executor::{PipelineExecutor, ProcessingStep};
// Import necessary filters (adjust if steps change)
// {{ Remove ArrowNativeType import if no longer needed directly here }}
// use arrow::datatypes::ArrowNativeType;
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
use TextBlaster::pipeline::filters::{
    C4QualityFilter, GopherQualityFilter, GopherRepetitionFilter,
};
// If GopherQualityFilter uses it
use serde_json;
use std::path::PathBuf;
use std::sync::Arc; // To share the executor across potential concurrent tasks
use std::time::Duration;
use tokio::time::sleep; // {{ Add serde_json for result serialization }}
use tracing::{error, info, warn, debug, instrument, info_span}; // Added tracing
use tracing_subscriber::{fmt, EnvFilter}; // Added tracing_subscriber

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

    // {{ Add argument for pipeline configuration file }}
    /// Path to the pipeline configuration YAML file.
    #[arg(short = 'c', long, default_value = "config/pipeline_config.yaml")]
    pipeline_config: PathBuf,

    /// Optional: Port for the Prometheus metrics HTTP endpoint
    #[arg(long)]
    metrics_port: Option<u16>,
}

// --- Prometheus Metrics ---
use once_cell::sync::Lazy;
use prometheus::{
    register_counter, register_gauge, register_histogram, Counter, Encoder, Gauge, Histogram, TextEncoder,
};

static TASKS_PROCESSED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!("worker_tasks_processed_total", "Total number of tasks processed by the worker.")
        .expect("Failed to register worker_tasks_processed_total counter")
});

static TASKS_FILTERED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!("worker_tasks_filtered_total", "Total number of tasks filtered by the pipeline.")
        .expect("Failed to register worker_tasks_filtered_total counter")
});

static TASKS_FAILED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!("worker_tasks_failed_total", "Total number of tasks that resulted in a pipeline error.")
        .expect("Failed to register worker_tasks_failed_total counter")
});

static TASK_DESERIALIZATION_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!("worker_task_deserialization_errors_total", "Total number of errors deserializing incoming task messages.")
        .expect("Failed to register worker_task_deserialization_errors_total counter")
});

static OUTCOME_PUBLISH_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!("worker_outcome_publish_errors_total", "Total number of errors publishing outcome messages.")
        .expect("Failed to register worker_outcome_publish_errors_total counter")
});

static TASK_PROCESSING_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "worker_task_processing_duration_seconds",
        "Histogram of task processing durations (from message receipt to outcome published/error)."
    ).expect("Failed to register worker_task_processing_duration_seconds histogram")
});

static ACTIVE_PROCESSING_TASKS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("worker_active_processing_tasks", "Number of tasks currently being processed concurrently.")
        .expect("Failed to register worker_active_processing_tasks gauge")
});


// Axum handler for /metrics
async fn metrics_handler() -> (axum::http::StatusCode, String) {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("Could not encode prometheus metrics: {}", e);
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Could not encode prometheus metrics: {}", e));
    }
    match String::from_utf8(buffer) {
        Ok(s) => (axum::http::StatusCode::OK, s),
        Err(e) => {
            error!("Prometheus metrics UTF-8 error: {}", e);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Prometheus metrics UTF-8 error: {}", e))
        }
    }
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
                info!("Successfully connected to RabbitMQ at {}", addr);
                return Ok(conn);
            }
            Err(e) => {
                attempts += 1;
                error!(
                    attempt = attempts,
                    error = %e,
                    "Failed to connect to RabbitMQ. Retrying in 5 seconds..."
                );
                if attempts >= 5 {
                    return Err(e);
                }
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// {{ Add the new function to build pipeline from configuration }}
/// Builds the processing pipeline based on the configuration read from YAML.
#[instrument(skip(config), fields(num_steps = config.pipeline.len()))]
fn build_pipeline_from_config(config: &PipelineConfig) -> Result<Vec<Box<dyn ProcessingStep>>> {
    let mut steps: Vec<Box<dyn ProcessingStep>> = Vec::new();
    info!("Building pipeline from configuration...");

    for (i, step_config) in config.pipeline.iter().enumerate() {
        let step_span = info_span!("pipeline_step", index = i, type = step_config.name());
        let _enter = step_span.enter();

        let step: Box<dyn ProcessingStep> = match step_config {
            StepConfig::C4QualityFilter(params) => {
                debug!(params = ?params, "Adding C4QualityFilter");
                Box::new(C4QualityFilter::new(
                    params.min_sentences,
                    params.min_words_per_sentence,
                    params.max_word_length,
                ))
            }
            StepConfig::GopherRepetitionFilter(params) => {
                debug!(params = ?params, "Adding GopherRepetitionFilter");
                Box::new(GopherRepetitionFilter::new(
                    params.dup_line_frac,
                    params.dup_para_frac,
                    params.dup_line_char_frac,
                    params.dup_para_char_frac,
                    params.top_n_grams.clone(), // Clone the vec
                    params.dup_n_grams.clone(), // Clone the vec
                ))
            }
            StepConfig::GopherQualityFilter(params) => {
                debug!(params = ?params, "Adding GopherQualityFilter");
                Box::new(GopherQualityFilter::new(
                    params.min_doc_words,
                    params.max_doc_words,
                    params.min_avg_word_length,
                    params.max_avg_word_length,
                    params.max_symbol_word_ratio,
                    params.max_bullet_lines_ratio,
                    params.max_ellipsis_lines_ratio,
                    params.max_non_alpha_words_ratio,
                    params.min_stop_words,
                    params.stop_words.clone(), // Clone the Option<Vec<String>>
                ))
            } // Add cases for other StepConfig variants here if you define more
        };
        steps.push(step);
        info!("Added step: {}", step_config.name());
    }

    if steps.is_empty() {
        warn!("Warning: Building an empty pipeline from configuration!");
    } else {
        info!("Pipeline built successfully with {} steps.", steps.len());
    }
    Ok(steps)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing subscriber
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set
    fmt::Subscriber::builder().with_env_filter(filter).init();

    // --- Optional: Start Metrics Endpoint ---
    if let Some(port) = args.metrics_port {
        let app = axum::Router::new().route("/metrics", axum::routing::get(metrics_handler));
        let listener_addr = format!("0.0.0.0:{}", port);
        info!("Metrics endpoint will be available at http://{}/metrics", listener_addr);

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

    info!("Worker starting.");
    info!(
        "Loading pipeline configuration from: {}",
        args.pipeline_config.display()
    );
    info!(
        "Consuming from queue '{}', publishing outcomes to '{}' @ {}",
        args.task_queue, args.results_queue, args.amqp_addr
    );
    info!("Prefetch count: {}", args.prefetch_count);

    // {{ Load and parse the pipeline configuration }}
    let pipeline_config: PipelineConfig = load_pipeline_config(&args.pipeline_config)?;

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

    // 4. Create the Pipeline Executor using the configuration
    // {{ Replace temporary variable with call to build_pipeline_from_config }}
    let pipeline_steps = build_pipeline_from_config(&pipeline_config)?;
    // The warning inside build_pipeline_from_config already covers the empty case
    let executor = Arc::new(PipelineExecutor::new(pipeline_steps));

    // 5. Start Consuming Messages from the task queue
    let consumer_tag = format!(
        "worker-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp()
    );
    info!(consumer_tag = %consumer_tag, "Worker started consuming tasks. Waiting for messages...");

    let mut consumer = consume_channel
        .basic_consume(
            &args.task_queue,
            &consumer_tag,
            BasicConsumeOptions::default(), // auto_ack: false (default)
            FieldTable::default(),
        )
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to start consuming: {}", e)))?;

    // This println! is now an info! log above.
    // println!("Worker started consuming tasks. Waiting for messages...");

    // 6. Process messages from the stream
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let executor_clone = Arc::clone(&executor);
                let publish_channel_clone = publish_channel.clone();
                let results_queue_name = args.results_queue.clone();

                // Spawn a Tokio task to process the message concurrently
                tokio::spawn(async move {
                    ACTIVE_PROCESSING_TASKS.inc(); // Increment gauge when task starts
                    let processing_timer = TASK_PROCESSING_DURATION_SECONDS.start_timer(); // Start timer

                    let result = match serde_json::from_slice::<TextDocument>(&delivery.data) {
                        Ok(doc) => {
                            let original_doc_id = doc.id.clone(); 
                            
                            let task_span = info_span!("process_task", doc_id = %original_doc_id, delivery_tag = %delivery.delivery_tag);
                            let _enter = task_span.enter();

                            debug!("Processing document"); 
                            match executor_clone.run_single_async(doc).await {
                                Ok(processed_doc) => {
                                    debug!(processed_doc_id = %processed_doc.id, "Successfully processed document"); 
                                    TASKS_PROCESSED_TOTAL.inc(); // Increment success counter
                                    Some(ProcessingOutcome::Success(processed_doc))
                                }
                                Err(pipeline_error) => {
                                    if let PipelineError::StepError { step_name, source } = pipeline_error {
                                        match *source {
                                            PipelineError::DocumentFiltered { document, reason, } => {
                                                info!(filtered_doc_id = %document.id, %step_name, %reason, "Document was filtered"); 
                                                TASKS_FILTERED_TOTAL.inc(); // Increment filtered counter
                                                Some(ProcessingOutcome::Filtered { document, reason })
                                            }
                                            other_error => {
                                                error!(%step_name, error = %other_error, "Pipeline step failed");
                                                TASKS_FAILED_TOTAL.inc(); // Increment failed counter
                                                None // Don't send outcome for pipeline errors
                                            }
                                        }
                                    } else {
                                        error!(error = %pipeline_error, "Unexpected pipeline error");
                                        TASKS_FAILED_TOTAL.inc(); // Increment failed counter
                                        None // Don't send outcome
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                delivery_tag = %delivery.delivery_tag,
                                error = %e,
                                payload = %String::from_utf8_lossy(&delivery.data),
                                "Failed to deserialize task message"
                            );
                            TASK_DESERIALIZATION_ERRORS_TOTAL.inc(); // Increment deserialization error counter
                            None // No outcome on deserialization error
                        }
                    };

                    // Send outcome back to producer if it's Success or Filtered
                    if let Some(actual_outcome) = result { // Use the result variable
                        match serde_json::to_vec(&actual_outcome) {
                            Ok(payload) => {
                                let publish_confirm = publish_channel_clone
                                    .basic_publish(
                                        "", 
                                        &results_queue_name,
                                        BasicPublishOptions::default(),
                                        &payload,
                                        AMQPProperties::default().with_delivery_mode(2), 
                                    )
                                    .await;

                                match publish_confirm {
                                    Ok(confirmation) => match confirmation.await { 
                                        Ok(_) => debug!("Published outcome"), 
                                        Err(e) => {
                                            error!(error = %e, "Failed publish confirmation for outcome");
                                            OUTCOME_PUBLISH_ERRORS_TOTAL.inc(); // Increment publish error counter
                                        }
                                    },
                                    Err(e) => {
                                        error!(error = %e, "Failed to initiate publish for outcome");
                                        OUTCOME_PUBLISH_ERRORS_TOTAL.inc(); // Increment publish error counter
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to serialize outcome");
                                // Consider a metric for outcome serialization errors if needed
                            }
                        }
                    }

                    // Acknowledge the original task message regardless of outcome or error
                    if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                        error!(error = %ack_err, "Failed to ack task message");
                        // Consider a metric for ACK errors if needed
                    }

                    processing_timer.observe_duration(); // Observe processing duration
                    ACTIVE_PROCESSING_TASKS.dec(); // Decrement gauge when task finishes
                });
            }
            Err(e) => {
                error!(error = %e, "Error receiving task message from consumer stream. Worker will stop.");
                break;
            }
        }
    }

    info!("Worker stopped consuming tasks.");

    // Optional: Graceful shutdown of channels and connection
    // Note: Depending on error handling, channels might already be closed.
    // Use `close` method with appropriate code and reason.
    // let _ = consume_channel.close(200, "Worker shutting down normally").await;
    // let _ = publish_channel.close(200, "Worker shutting down normally").await;
    // let _ = conn.close(200, "Worker shutting down normally").await;

    Ok(())
}
