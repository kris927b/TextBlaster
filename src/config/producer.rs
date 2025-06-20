// --- Command-Line Arguments Struct ---
// Moved from src/bin/producer.rs to make it accessible by library tests and other binaries if needed.
use clap::Parser; // Add clap::Parser import

#[derive(Parser, Debug, Clone)] // Added Clone for testability if needed
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the input Parquet file
    #[arg(short, long)]
    pub input_file: String,

    /// Text column name in the Parquet file
    #[arg(long, default_value = "text")]
    pub text_column: String,

    /// Optional ID column name in the Parquet file
    #[arg(long)]
    pub id_column: Option<String>,

    /// RabbitMQ connection string (e.g., amqp://guest:guest@localhost:5672/%2f)
    #[arg(short, long, default_value = "amqp://guest:guest@localhost:5672/%2f")]
    pub amqp_addr: String,

    /// Name of the queue to publish tasks to
    #[arg(short = 'q', long, default_value = "task_queue")]
    pub task_queue: String,

    /// Name of the queue to consume results/outcomes from
    #[arg(short = 'r', long, default_value = "results_queue")]
    pub results_queue: String,

    /// Prefetch count (how many messages to buffer locally)
    #[arg(long, default_value_t = 10)] // Adjust based on task duration/resources
    pub prefetch_count: u16,

    /// Path to the output Parquet file
    #[arg(short = 'o', long, default_value = "output_processed.parquet")]
    pub output_file: String,

    /// Path to the excluded output Parquet file
    #[arg(short = 'e', long, default_value = "excluded.parquet")]
    pub excluded_file: String,

    /// Optional: Port for the Prometheus metrics HTTP endpoint
    #[arg(long)]
    pub metrics_port: Option<u16>,
}
