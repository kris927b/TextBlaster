// src/bin/producer.rs

use TextBlaster::config::Args;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle}; // Removed HumanDuration
// lapin::options are used by producer_logic, not directly here anymore for these specific ones
// use lapin::{
//     options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
//     types::FieldTable,
// };
// std::time::Instant is used by producer_logic
use tracing::{error, info, warn};
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
// TextBlaster::data_model imports are used by producer_logic
// TextBlaster::error::{PipelineError, Result} is used
use TextBlaster::error::{PipelineError, Result};
// TextBlaster::pipeline::writers::parquet_writer::ParquetWriter is used by producer_logic
use TextBlaster::utils::common::connect_rabbitmq;
use TextBlaster::utils::prometheus_metrics::setup_prometheus_metrics;
// chrono::Utc is used by producer_logic
// TextBlaster::utils::prometheus_metrics::* is used by producer_logic

// const PARQUET_WRITE_BATCH_SIZE: usize = 500; // This const is now in producer_logic.rs

// Args struct is now imported from TextBlaster::config::Args

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

// publish_tasks and aggregate_results functions are now in TextBlaster::producer_logic

// setup_prometheus_metrics and metrics_handler removed, now imported from utils (already done)

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize tracing subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set

    // Setup file logging
    let file_appender = rolling::daily("./log", "producer.log");
    let (non_blocking_file_writer, _guard) = non_blocking(file_appender);

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
    let conn = connect_rabbitmq(&args.amqp_addr).await?; // This returns a lapin::Connection

    // Create channels for publishing and consuming results
    let task_publish_channel = conn.create_channel().await.map_err(PipelineError::from)?;
    let results_consume_channel = conn.create_channel().await.map_err(PipelineError::from)?;

    // Optionally, set task_publish_channel to confirm mode if desired for all publishes
    // task_publish_channel.confirm_select(lapin::options::ConfirmSelectOptions::default()).await
    //    .map_err(|e| PipelineError::QueueError(format!("Failed to set task channel to confirm mode: {}", e)))?;


    // --- Progress Bars ---
    let publishing_pb_template =
        "{spinner:.green} [{elapsed_precise}] {msg} Tasks published: {pos} ({per_sec}, ETA: {eta})";
    let publishing_pb = create_progress_bar(0, "Publishing tasks", publishing_pb_template);

    // 2. Publish Tasks - now passing the channel directly
    let published_count = match TextBlaster::producer_logic::publish_tasks(&args, &task_publish_channel, &publishing_pb).await {
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
    // aggregate_results now takes the results_consume_channel directly
    let (outcomes_received_count, success_count, filtered_count) =
        match TextBlaster::producer_logic::aggregate_results(&args, &results_consume_channel, published_count, &aggregation_pb).await {
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
