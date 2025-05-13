# TextBlaster: A Distributed Text Processing Pipeline

[![Rust Build](https://img.shields.io/badge/build-passing-brightgreen)]()
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)]()

`TextBlaster` is a Rust-based tool designed for efficient, distributed processing of large text datasets stored in Parquet format. It leverages a producer-worker architecture using RabbitMQ for task queuing, enabling scalable text cleaning, filtering, and transformation pipelines.

This tool is particularly useful for preprocessing large NLP datasets, such as web scrapes or Common Crawl data, by applying quality filters (like C4 and Gopher heuristics) in parallel.

## Features

*   **Distributed Processing:** Uses a producer/worker model with RabbitMQ for scalable task distribution.
*   **Parquet I/O:** Reads input data from Parquet files and writes processed results back to Parquet.
*   **Configurable Pipeline:** Define a sequence of processing steps (filters, transformations) to apply to each text document.
*   **Implemented Filters:** Includes common NLP dataset quality filters:
    *   `C4QualityFilter`: Basic quality checks (sentence count, word count per sentence, word length, punctuation).
    *   `GopherRepetitionFilter`: Detects and filters documents with excessive repetition of lines, paragraphs, or n-grams.
    *   `GopherQualityFilter`: Applies heuristics from the Gopher paper (word count, average word length, symbol ratios, stop words, etc.).
*   **Asynchronous:** Built with `tokio` and `async-trait` for efficient I/O and concurrent processing.
*   **Command-Line Interface:** Easy-to-use CLI powered by `clap` for configuring and running the producer and workers.

## Architecture

The system consists of two main components: a `producer` and one or more `worker` instances, communicating via RabbitMQ.

```mermaid
graph LR
    A[Input Parquet File] --> P(Producer);
    P -->|"Task (JSON Doc)"| Q1(RabbitMQ Task Queue);
    Q1 -->|"Task (JSON Doc)"| W1(Worker 1);
    Q1 -->|"Task (JSON Doc)"| W2(Worker 2);
    Q1 -->|"Task (JSON Doc)"| WN(Worker N);
    W1 -->|"Result (JSON Doc)"| Q2(RabbitMQ Results Queue);
    W2 -->|"Result (JSON Doc)"| Q2;
    WN -->|"Result (JSON Doc)"| Q2;
    Q2 -->|"Result (JSON Doc)"| P;
    P --> O[Output Parquet File];
```

1.  **Producer:**
    *   Reads `TextDocument` records from the input Parquet file.
    *   Serializes each document into JSON.
    *   Publishes each JSON document as a task message to the RabbitMQ `task_queue`.
    *   Waits for results from the `results_queue`.
    *   Consumes result messages (processed JSON documents).
    *   Deserializes results and writes them in batches to the output Parquet file.
2.  **Worker(s):**
    *   Connects to RabbitMQ and consumes task messages from the `task_queue`.
    *   Deserializes the JSON task message back into a `TextDocument`.
    *   Processes the document through a pre-defined pipeline of `ProcessingStep`s (e.g., filters).
    *   If the document passes all steps, it serializes the processed `TextDocument` into JSON.
    *   Publishes the JSON result message to the RabbitMQ `results_queue`.
    *   Acknowledges the original task message. If processing fails or a filter removes the document, the task is still acknowledged, but no result is sent.

## Prerequisites

*   **Rust:** Version 1.65 or later (due to `edition = "2021"` and dependencies). Install via [rustup](https://rustup.rs/).
*   **Cargo:** Included with Rust installation.
*   **RabbitMQ Server:** A running RabbitMQ instance. You can run one easily using Docker:
    ```bash
    docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
    ```
    The default connection string `amqp://guest:guest@localhost:5672/%2f` should work with this setup.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/kris927b/TextBlaster.git
    cd TextBlaster
    ```
2.  **Build the binaries:**
    ```bash
    cargo build --release
    ```
    This will create optimized executables at `target/release/producer` and `target/release/worker`.

## Configuration

Configuration is primarily handled via command-line arguments for both the `producer` and `worker`.

*   **Producer:** Specify input/output paths, column names, and RabbitMQ details.
*   **Worker:** Specify RabbitMQ details and queue names.

**Pipeline Steps:** The sequence of processing steps applied by the worker is currently defined within the `build_pipeline` function in `src/bin/worker.rs`. To customize the pipeline (add, remove, or reorder steps, or change filter parameters), you will need to modify this function and recompile.

**Input Parquet Config:** The `ParquetInputConfig` struct (`src/config.rs`) defines how the producer reads the input Parquet file (path, column names, batch size). These values are passed via CLI arguments to the producer.

## Usage

Ensure your RabbitMQ server is running before starting the producer or workers.

1.  **Start Worker(s):**
    Open one or more terminal windows and run the worker executable. You can run multiple workers concurrently on the same or different machines to parallelize processing.
    ```bash
    ./target/release/worker \
        --amqp-addr "amqp://guest:guest@localhost:5672/%2f" \
        --task-queue "task_queue" \
        --results-queue "results_queue" \
        --prefetch-count 10 # Optional: Adjust based on task processing time
    ```
    *   `--amqp-addr` (or `-a`): RabbitMQ connection string.
    *   `--task-queue` (or `-t`): Name of the queue to consume tasks from (default: `task_queue`).
    *   `--results-queue` (or `-r`): Name of the queue to publish results to (default: `results_queue`).
    *   `--prefetch-count`: How many messages a worker receives at once.

2.  **Start the Producer:**
    In a separate terminal, run the producer executable to start reading the input file and dispatching tasks.
    ```bash
    ./target/release/producer \
        --input-file "/path/to/your/input_data.parquet" \
        --output-file "/path/to/your/processed_output.parquet" \
        --text-column "text" \
        --id-column "document_id" `# Optional: Specify if you have an ID column` \
        --amqp-addr "amqp://guest:guest@localhost:5672/%2f" \
        --task-queue "task_queue" \
        --results-queue "results_queue"
    ```
    *   `--input-file` (or `-i`): Path to the input Parquet file.
    *   `--output-file` (or `-o`): Path where the processed Parquet file will be written (default: `output_processed.parquet`).
    *   `--text-column`: Name of the column containing the main text content (default: `text`).
    *   `--id-column`: (Optional) Name of the column to use as the document ID. If not provided, IDs are generated based on file path and row number.
    *   `--amqp-addr` (or `-a`): RabbitMQ connection string.
    *   `--task-queue` (or `-q`): Name of the queue to publish tasks to (default: `task_queue`).
    *   `--results-queue` (or `-r`): Name of the queue to consume results from (default: `results_queue`).

The producer will read the input, send tasks, wait for all corresponding results, write the output Parquet file, and then exit. Workers will continue running until stopped manually (e.g., with Ctrl+C).

## Monitoring and Metrics

Both the producer and worker binaries expose an optional Prometheus metrics HTTP endpoint. This allows you to monitor the application's performance and state using Prometheus and visualization tools like Grafana.

To enable the metrics endpoint, use the `--metrics-port` command-line argument when starting the producer or worker:

```bash
# Run producer with metrics on port 8080
./target/release/producer --input-file ... --metrics-port 8080

# Run worker with metrics on port 8081
./target/release/worker --metrics-port 8081
```

Replace `8080` and `8081` with your desired ports.

Once enabled, you can access the metrics by sending an HTTP GET request to the `/metrics` path on the specified port (e.g., `http://localhost:8080/metrics` or `http://your_server_ip:8081/metrics`).

The exposed metrics include (but are not limited to):

**Producer Metrics:**
*   `producer_tasks_published_total`: Total number of tasks successfully published to the queue.
*   `producer_task_publish_errors_total`: Total errors during task publishing (serialization, broker issues).
*   `producer_results_received_total`: Total number of outcomes received from the results queue.
*   `producer_results_success_total`: Total number of successfully processed documents (from outcomes).
*   `producer_results_filtered_total`: Total number of documents filtered by workers (from outcomes).
*   `producer_result_deserialization_errors_total`: Total errors deserializing outcome messages.
*   `producer_active_tasks_in_flight`: Number of tasks published but not yet resolved by workers.
*   `producer_task_publishing_duration_seconds`: Histogram of task publishing latencies.

**Worker Metrics:**
*   `worker_tasks_processed_total`: Total number of tasks successfully processed by the worker pipeline.
*   `worker_tasks_filtered_total`: Total number of tasks filtered out by the worker pipeline.
*   `worker_tasks_failed_total`: Total number of tasks that resulted in a pipeline error.
*   `worker_task_deserialization_errors_total`: Total errors deserializing incoming task messages.
*   `worker_outcome_publish_errors_total`: Total errors publishing outcome messages back to the producer.
*   `worker_task_processing_duration_seconds`: Histogram of task processing durations.
*   `worker_active_processing_tasks`: Number of tasks currently being processed concurrently by the worker.

## Pipeline Structure

The core processing logic resides in the pipeline executor (`src/executor.rs`) which runs a series of steps implementing the `ProcessingStep` trait. Each step takes a `TextDocument` and returns a `Result<TextDocument>`. If any step returns an `Err`, processing for that document stops. A specific `PipelineError::DocumentFiltered` error is used to indicate that a document was intentionally filtered out.

The current pipeline executed by the worker (in `src/bin/worker.rs`) includes:

1.  **`C4QualityFilter`**: (`src/pipeline/filters/c4_filters.rs`) Checks for minimum sentence count, minimum words per sentence, maximum word length, and sentence-ending punctuation.
2.  **`GopherRepetitionFilter`**: (`src/pipeline/filters/gopher_rep.rs`) Filters documents based on thresholds for duplicate lines/paragraphs (by count and character fraction) and duplicate n-grams.
3.  **`GopherQualityFilter`**: (`src/pipeline/filters/gopher_quality.rs`) Filters documents based on word count, average word length, symbol ratios, bullet/ellipsis line ratios, alphabetic ratio, and stop-word presence.

Refer to the source files in `src/pipeline/filters/` for detailed implementation and parameter explanations.

## Data Model

The central data structure processed by the pipeline is `TextDocument` (`src/data_model.rs`):

```rust
pub struct TextDocument {
    pub id: String,                     // Unique identifier
    pub source: String,                 // Original source (e.g., input file path)
    pub content: String,                // The main text content
    pub metadata: HashMap<String, String>, // Key-value store for metadata or intermediate results
}
```

When writing to the output Parquet file, the `metadata` field is serialized to a JSON string.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs, feature requests, or improvements.

<!-- TODO: Add more specific contribution guidelines if needed -->

## License

This project is licensed under:

*   Apache License, Version 2.0, ([LICENSE](LICENSE))
