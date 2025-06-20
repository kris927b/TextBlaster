// src/bin/worker.rs

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::{Duration, Instant};
use TextBlaster::config::worker::Args;
use TextBlaster::worker_logic::{build_pipeline_from_config, process_tasks_with_executor}; // Added for progress bar // Added for progress bar speed calculation
                                                                                          // {{ Use the new load_pipeline_config function }}
use TextBlaster::config::pipeline::{load_pipeline_config, PipelineConfig};
use TextBlaster::error::{PipelineError, Result}; // Use the library's Result type
use TextBlaster::executor::PipelineExecutor;
// Import necessary filters (adjust if steps change)
// {{ Remove ArrowNativeType import if no longer needed directly here }}
// use arrow::datatypes::ArrowNativeType;

use TextBlaster::utils::common::connect_rabbitmq; // Updated for shared functions
use TextBlaster::utils::prometheus_metrics::*;

use std::sync::Arc; // To share the executor across potential concurrent tasks
                    // {{ Add serde_json for result serialization }}
use tracing::{error, info}; // Added tracing
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer}; // Added tracing_subscriber // Added for file logging

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.validate_config {
        match load_pipeline_config(&args.pipeline_config) {
            Ok(_) => {
                // Note: Tracing might not be initialized here.
                // Consider simple println for this specific validation output.
                println!(
                    "Configuration '{}' is valid.",
                    args.pipeline_config.display()
                );
                std::process::exit(0);
            }
            Err(e) => {
                // Note: Tracing might not be initialized here.
                // Consider simple eprintln for this specific validation output.
                eprintln!(
                    "Configuration '{}' is invalid: {}",
                    args.pipeline_config.display(),
                    e
                );
                std::process::exit(1);
            }
        }
    }

    // Initialize tracing subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set

    // Setup file logging
    let file_appender = rolling::daily("./log", "worker.log");

    let (non_blocking_file_writer, _guard) = non_blocking(file_appender);

    // Configure the console layer
    let console_layer = fmt::layer()
        .with_writer(std::io::stdout) // Write to stdout
        .with_filter(EnvFilter::new("warn")); // Only info and above for console

    // Configure the file logging layer
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
        // Depending on policy, just logging.
    }

    // --- Progress Bar Setup ---
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
            .template("{spinner:.green} {msg}")
            .expect("Failed to create progress style"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_message("Initializing...");

    let progress_updater_handle = tokio::spawn({
        let pb_clone = pb.clone();
        async move {
            let mut last_processed_count = 0.0;
            let mut last_update_time = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let current_processed_total = TASKS_PROCESSED_TOTAL.get();
                let current_filtered_total = TASKS_FILTERED_TOTAL.get();
                // Sum of pipeline errors and deserialization errors for a total error count
                let current_errored_total =
                    TASKS_FAILED_TOTAL.get() + TASK_DESERIALIZATION_ERRORS_TOTAL.get();

                let now = Instant::now();
                let duration_since_last_update = (now - last_update_time).as_secs_f64();
                let processed_since_last = current_processed_total - last_processed_count;

                let speed = if duration_since_last_update > 0.0 {
                    processed_since_last / duration_since_last_update
                } else {
                    0.0
                };

                pb_clone.set_message(format!(
                    "Processed: {}, Filtered: {}, Errored: {} | Speed: {:.2} docs/sec",
                    current_processed_total, current_filtered_total, current_errored_total, speed
                ));

                last_processed_count = current_processed_total;
                last_update_time = now;
            }
        }
    });
    // --- End Progress Bar Setup ---

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
    let task_processing_result = process_tasks_with_executor(&args, &conn, executor).await;

    // Stop the progress bar and the updater task
    pb.finish_with_message("Processing finished or interrupted."); // Updated message
    progress_updater_handle.abort(); // Stop the updater task

    if let Err(e) = task_processing_result {
        error!("Error during task processing: {}", e);
        // conn.close() could be called here if specific cleanup is needed,
        // but often relying on drop is fine for error scenarios.
        return Err(e);
    }

    // If process_tasks returns Ok, it means the consumer stream ended gracefully.
    info!("Worker main function finished successfully.");
    Ok(())
}
