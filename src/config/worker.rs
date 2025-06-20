use std::path::PathBuf;

use clap::Parser;

// Define command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// RabbitMQ connection string (e.g., amqp://guest:guest@localhost:5672/%2f)
    #[arg(short, long, default_value = "amqp://guest:guest@localhost:5672/%2f")]
    pub amqp_addr: String,

    /// Name of the queue to consume tasks from
    #[arg(short = 'q', long, default_value = "task_queue")]
    // Use short arg 'q' consistent with producer
    pub task_queue: String,

    /// Name of the queue to publish results/outcomes to
    #[arg(short = 'r', long, default_value = "results_queue")]
    // Use short arg 'r' consistent with producer
    pub results_queue: String,

    /// Prefetch count (how many messages to buffer locally)
    #[arg(long, default_value_t = 10)] // Adjust based on task duration/resources
    pub prefetch_count: u16,

    // {{ Add argument for pipeline configuration file }}
    /// Path to the pipeline configuration YAML file.
    #[arg(short = 'c', long, default_value = "config/pipeline_config.yaml")]
    pub pipeline_config: PathBuf,

    /// Optional: Port for the Prometheus metrics HTTP endpoint
    #[arg(long)]
    pub metrics_port: Option<u16>,

    /// Validate the pipeline configuration and exit
    #[arg(long)]
    pub validate_config: bool,
}
