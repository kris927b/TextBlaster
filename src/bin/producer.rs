use TextBlaster::error::Result;
use TextBlaster::server::run_server;
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")); // Default to info if RUST_LOG is not set

    // Setup file logging
    let file_appender = rolling::daily("./log", "producer.log");
    let (non_blocking_file_writer, _guard) = non_blocking(file_appender);

    // Configure the console layer
    let console_layer = fmt::layer()
        .with_writer(std::io::stdout) // Write to stdout
        .with_filter(EnvFilter::new("info")); // Only info and above for console

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
        .init();

    // Run the server
    run_server().await;

    Ok(())
}
