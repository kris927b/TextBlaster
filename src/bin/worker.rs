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
    C4BadWordsFilter, C4QualityFilter, GopherQualityFilter, GopherRepetitionFilter,
    LanguageDetectionFilter,
};
use TextBlaster::utils::utils::{connect_rabbitmq, setup_prometheus_metrics}; // Using shared setup_prometheus_metrics
use TextBlaster::utils::prometheus_metrics::*; // Import shared metrics

use std::path::PathBuf;
use std::sync::Arc; // To share the executor across potential concurrent tasks
use std::time::Duration;
use tokio::time::sleep; // {{ Add serde_json for result serialization }}
use tracing::{debug, error, info, info_span, instrument, warn}; // Added tracing
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

// --- Prometheus Metrics (now imported from TextBlaster::utils::prometheus_metrics) ---
// The local static definitions and specific prometheus imports are removed.

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
                    params.split_paragraph,
                    params.remove_citations,
                    params.filter_no_terminal_punct,
                    params.min_num_sentences,
                    params.min_words_per_line,
                    params.max_word_length,
                    params.filter_lorem_ipsum,
                    params.filter_javascript,
                    params.filter_curly_bracket,
                    params.filter_policy,
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
            }
            StepConfig::LanguageDetectionFilter(params) => {
                debug!(params = ?params, "Adding LanguageDetectionFilter");
                Box::new(LanguageDetectionFilter::new(
                    params.min_confidence,
                    params.allowed_languages.clone(),
                ))
            }
            StepConfig::C4BadWordsFilter(params) => {
                debug!(params = ?params, "Adding C4BadWordsFilter");
                Box::new(C4BadWordsFilter::new(params.clone()))
            }
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

async fn process_tasks(
    args: &Args,
    conn: &lapin::Connection,
    executor: Arc<PipelineExecutor>,
) -> Result<(), PipelineError> {
    // Create two channels: one for consuming tasks, one for publishing results/outcomes
    let consume_channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!("Worker failed to create consume channel: {}", e))
    })?;
    let publish_channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!("Worker failed to create publish channel: {}", e))
    })?;

    // Declare the task queue (ensure durability matches producer)
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

    // Declare the results queue (also durable)
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

    // Set Quality of Service (Prefetch Count) on the consume channel
    consume_channel
        .basic_qos(args.prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to set QoS: {}", e)))?;

    // Start Consuming Messages from the task queue
    let consumer_tag = format!(
        "worker-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp() // Ensure chrono::Utc is imported
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

    // Process messages from the stream
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let executor_clone = Arc::clone(&executor);
                let publish_channel_clone = publish_channel.clone();
                let results_queue_name = args.results_queue.clone();
                let worker_id_tag = consumer_tag.clone(); // Use the specific consumer_tag for this worker instance

                tokio::spawn(async move {
                    ACTIVE_PROCESSING_TASKS.inc();
                    let processing_timer = TASK_PROCESSING_DURATION_SECONDS.start_timer();

                    let result: Option<ProcessingOutcome> = match serde_json::from_slice::<
                        TextDocument,
                    >(
                        &delivery.data
                    ) {
                        Ok(doc) => {
                            let original_doc_id = doc.id.clone();
                            let task_span = info_span!("process_task", doc_id = %original_doc_id, delivery_tag = %delivery.delivery_tag);
                            let _enter = task_span.enter();
                            debug!("Processing document");

                            match executor_clone.run_single_async(doc.clone()).await {
                                Ok(processed_doc) => {
                                    debug!(processed_doc_id = %processed_doc.id, "Successfully processed document");
                                    TASKS_PROCESSED_TOTAL.inc();
                                    Some(ProcessingOutcome::Success(processed_doc))
                                }
                                Err(pipeline_error) => {
                                    match pipeline_error {
                                        PipelineError::DocumentFiltered { document, reason } => {
                                            info!(filtered_doc_id = %document.id, %reason, "Document was filtered");
                                            TASKS_FILTERED_TOTAL.inc();
                                            Some(ProcessingOutcome::Filtered { document, reason })
                                        }
                                        other_error => {
                                            error!(error = %other_error, "Pipeline execution failed");
                                            TASKS_FAILED_TOTAL.inc();
                                            Some(ProcessingOutcome::Error {
                                                document: doc,
                                                error_message: other_error.to_string(),
                                                worker_id: worker_id_tag,
                                            })
                                        }
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
                            TASK_DESERIALIZATION_ERRORS_TOTAL.inc();
                            None
                        }
                    };

                    if let Some(actual_outcome) = result {
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
                                            OUTCOME_PUBLISH_ERRORS_TOTAL.inc();
                                        }
                                    },
                                    Err(e) => {
                                        error!(error = %e, "Failed to initiate publish for outcome");
                                        OUTCOME_PUBLISH_ERRORS_TOTAL.inc();
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to serialize outcome");
                            }
                        }
                    }

                    if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                        error!(error = %ack_err, "Failed to ack task message");
                    }

                    processing_timer.observe_duration();
                    ACTIVE_PROCESSING_TASKS.dec();
                });
            }
            Err(e) => {
                error!(error = %e, "Error receiving task message from consumer stream. Worker will stop.");
                // This error will propagate up from process_tasks if the loop breaks
                return Err(PipelineError::QueueError(format!("Consumer stream error: {}", e)));
            }
        }
    }
    // If the loop finishes (e.g. queue deleted, channel closed gracefully), it's not necessarily an error.
    // Specific errors during consumption (like connection loss) would break the loop and return Err.
    info!("Worker stopped consuming tasks (consumer stream ended).");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing subscriber
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set
    fmt::Subscriber::builder().with_env_filter(filter).init();

    // Setup Prometheus Metrics Endpoint
    if let Err(e) = setup_prometheus_metrics(args.metrics_port).await {
        error!("Failed to start Prometheus metrics endpoint: {}", e);
        // Depending on policy, just logging.
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

    // Load and parse the pipeline configuration
    let pipeline_config: PipelineConfig = load_pipeline_config(&args.pipeline_config)?;
    let pipeline_steps = build_pipeline_from_config(&pipeline_config)?;
    let executor = Arc::new(PipelineExecutor::new(pipeline_steps));

    // Connect to RabbitMQ
    let conn = connect_rabbitmq(&args.amqp_addr)
        .await
        .map_err(|e| PipelineError::QueueError(format!("Worker failed to connect: {}", e)))?;

    // Process tasks
    if let Err(e) = process_tasks(&args, &conn, executor).await {
        error!("Error during task processing: {}", e);
        // conn.close() could be called here if specific cleanup is needed,
        // but often relying on drop is fine for error scenarios.
        return Err(e);
    }

    // If process_tasks returns Ok, it means the consumer stream ended gracefully or an unrecoverable error within it was handled.
    // The main function can now also be considered to have finished its primary work.
    info!("Worker main function finished.");
    Ok(())
}
