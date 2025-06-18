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

**Pipeline Configuration:** The sequence of processing steps applied by the worker is defined in the `config/pipeline_config.yaml` file. This allows for dynamic configuration of the pipeline without recompiling the worker binary.

The `pipeline_config.yaml` file specifies a list of processing steps to be executed in order. Each step is defined by its `type` and any necessary `parameters`.

Example `pipeline_config.yaml`:

```yaml
steps:
  - type: C4QualityFilter
    parameters:
      min_sentences: 3
      min_words_per_sentence: 5
      max_word_length: 100
      must_end_with_punct: true
  - type: GopherRepetitionFilter
    parameters:
      # ... gopher repetition parameters ...
  - type: LanguageDetectionFilter
    parameters:
      languages: ["en"]
```

To customize the pipeline, modify the `config/pipeline_config.yaml` file to add, remove, reorder steps, or change filter parameters.

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
4.  **`LanguageDetectionFilter`**: (`src/pipeline/filters/language_filter.rs`) Filters documents based on language. Uses the `whatlang` package for detection. 

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

## TextBlaster Development Roadmap

The roadmap for TextBlaster is divided into 6 phases, with subtasks to complete. Each subtask is supposed to be a self isolated PR, and once the phase is done a new major version can be released. Each phase is denoted by a theme, that explains the grouping of sub-tasks.

### **Phase 1: Foundational Robustness & Developer Experience**
**Theme:** Solidify the core application by enhancing stability, standardizing practices, and improving the development loop. This phase is about building a reliable and maintainable foundation.

1.  **Comprehensive Unit & Integration Testing:** Achieve >80% test coverage by writing unit tests for all filter logic and integration tests for the full producer-worker workflow. Use `testcontainers` to spin up a live RabbitMQ instance for end-to-end verification.
2.  **CI/CD Pipeline Enforcement:** Enhance the GitHub Actions workflow to run `cargo fmt --check` and `cargo clippy -- -D warnings` on all pull requests. Add `cargo audit` to check for security vulnerabilities and automate release drafts upon tagging.
3.  **Standardized Logging and Tracing:** Fully integrate the `tracing` crate, replacing all `println!` calls. Structure logs as JSON and add `doc_id` to tracing spans to correlate all messages for a specific document. Allow log level configuration via CLI.
4.  **Refined Error Handling & Messaging:** Improve the clarity of all user-facing error messages. Ensure that when a task fails in a worker, the propagated error clearly identifies the worker ID, the failing step, and the root cause.
5.  **Comprehensive Documentation:** Create a `docs/` folder with Markdown guides on architecture, configuration, and tutorials. Add `CONTRIBUTING.md` for new developers and ensure all public functions and structs have detailed doc comments (`///`).
6.  **Configuration Validation:** Implement a `--validate-config` flag and an automatic startup check to validate the `pipeline_config.yaml`. This check will catch syntax errors, schema violations, and logical issues (e.g., a filter running before its dependency). - *Done*

### **Phase 2: Core Pipeline Enhancement & Usability**
**Theme:** Improve the user experience and expand the core capabilities of the processing pipeline, making it more flexible and powerful for common tasks.

1.  **Improved CLI with Subcommands & Dry-Run Mode:** Refactor the CLI using `clap` to support subcommands (`producer run`, `worker run`, `validate-config`). Add a `--dry-run` flag to the producer to process a small sample and print results to the console without writing files.
2.  **Introduce Transformation Steps:** Extend the `ProcessingStep` trait to allow in-place modification of document content, not just filtering. Implement a `TextNormalizationStep` (e.g., lowercase, NFC normalization, whitespace removal) as the first example.
3.  **Abstract I/O Layer for Multiple Data Formats:** Refactor I/O logic behind `DocumentReader` and `DocumentWriter` traits. Implement support for reading and writing additional formats like JSON Lines (`.jsonl`) and CSV, selectable via CLI arguments.
4.  **Support for Rich, Nested Metadata:** Upgrade the `TextDocument` metadata from `HashMap<String, String>` to `HashMap<String, serde_json::Value>` to allow for complex, nested metadata structures from various data sources.
5.  **Implement Graceful Shutdown:** Ensure workers can gracefully complete in-flight tasks and acknowledge their messages before shutting down when receiving a termination signal (SIGTERM), minimizing data loss.
6.  **Local Development Environment:** Create a `docker-compose.yml` file and a `.env` file template to simplify the setup of a complete local development environment (RabbitMQ, producer, worker) with a single command.

### **Phase 3: Performance, Scalability & State Management**
**Theme:** Identify and eliminate performance bottlenecks, optimize resource usage, and introduce stateful capabilities to prepare the tool for massive-scale datasets.

1.  **Comprehensive Performance Profiling:** Use tools like `flamegraph`, `dhat`, and `tokio-console` to profile CPU and memory hotspots in both the producer and worker. Instrument each pipeline step with `tracing` spans to precisely measure its performance.
2.  **Optimize I/O with Asynchronous Operations:** Refactor the Parquet reader and writer to use `tokio::fs` and the asynchronous capabilities of the `arrow` and `parquet` crates, eliminating blocking I/O.
3.  **Optimize Message Payloads:** Investigate and implement strategies to reduce network overhead. Options include optional payload compression (e.g., Zstd) or a "pointer-based" mode where data is placed in shared storage and the message queue only carries a pointer.
4.  **Implement Job Progress Tracking & Resumability:** Implement a mechanism for the producer to save its state (e.g., last processed row ID). Add a `--resume` flag to allow it to recover from a crash and continue processing without starting over.
5.  **Introduce a Stateful Processing Step:** Integrate a key-value store like Redis or RocksDB and create a `StatefulProcessingStep` trait. This will enable stateful filters like document deduplication.
6.  **Worker Concurrency & Throughput Tuning:** Expose configuration to tune the number of concurrent processing tasks within a single worker, in addition to the RabbitMQ `prefetch_count`, to maximize resource utilization.

### **Phase 4: Advanced Processing Capabilities**
**Theme:** Evolve TextBlaster from a generic tool to a specialized platform for NLP data preparation by adding sophisticated, high-value processing steps.

1.  **Implement Stateful Document Deduplication:** Create a `DeduplicationFilter` using MinHash or SimHash algorithms. This step will leverage the stateful processing foundation to identify and filter near-duplicate documents across the entire dataset.
2.  **Add a PII Redaction & Filtering Step:** Implement a `PiiFilter` to detect and either remove documents containing Personally Identifiable Information or redact it (e.g., replacing emails with `[REDACTED]`).
3.  **Implement Advanced NLP-Based Filters:** Add a suite of more intelligent filters, potentially using `ort` for ONNX model inference. Examples include a `PerplexityFilter` for non-sensical text, a `ToxicityFilter`, and a `QualityScoreFilter`.
4.  **Support for Conditional Pipeline Execution:** Enhance the pipeline executor to support conditional logic in the configuration file, allowing a step to run only if a specific metadata condition is met (e.g., `if: metadata.language == "en"`).
5.  **Implement Data Sampling and Splitting:** Add a `SamplingStep` for random or stratified sampling. Enhance the producer/writer to split output into multiple files (e.g., train/val/test) based on flags added during processing.
6.  **Dead Letter Queue (DLQ) Management:** Configure RabbitMQ to route messages that repeatedly fail to a Dead Letter Queue. Provide a utility command to inspect, re-queue, or discard messages from the DLQ.

### **Phase 5: Ecosystem Integration & Orchestration**
**Theme:** Position TextBlaster within the broader MLOps and data engineering ecosystem, enabling deployment in modern, cloud-native environments.

1.  **Enable Cloud Storage Integration (S3, GCS, etc.):** Use a library like `object_store` to enable the producer and writer to read from and write to cloud storage buckets (e.g., `s3://my-bucket/data.parquet`) natively.
2.  **Containerize the Application:** Create official `Dockerfile`s for the `producer`, `worker`, and other binaries. Publish the images to a public container registry like GitHub Container Registry or Docker Hub.
3.  **Develop Kubernetes Deployment Configurations:** Create a Helm chart or Kustomize configuration to simplify the deployment, scaling, and management of a TextBlaster cluster on Kubernetes.
4.  **Abstract the Message Broker for Alternative Queues:** Refactor the RabbitMQ logic behind a `MessageBroker` trait and add a new implementation for another system like Apache Kafka or Redis Streams, making the queuing system configurable.
5.  **Decouple Result Aggregation:** Create a new `aggregator` binary responsible for consuming processed results and writing the final output files. This decouples the producer from the writing bottleneck, improving overall system throughput.
6.  **Develop a Python Client Library:** Create a thin Python wrapper that allows users to configure, launch, and monitor TextBlaster jobs from Python scripts or Jupyter notebooks, significantly lowering the barrier to entry for data scientists.

### **Phase 6: User-Facing Polish & Advanced Operations**
**Theme:** Finalize the user experience with advanced monitoring, reporting, and ease-of-use features that make the tool a pleasure to operate at scale.

1.  **Create a Web UI for Monitoring:** Develop a simple, optional web dashboard (e.g., using Axum) that consumes the Prometheus metrics endpoint to visualize key metrics like processing rates, queue depths, error rates, and worker status in real-time.
2.  **Enhance Operational Feedback & Reporting:** Improve the CLI progress bars (`indicatif`) to show live counts of succeeded, filtered, and errored tasks. At the end of a run, generate a `run_summary.json` file with final metrics and per-step filter counts.
3.  **Create a 'Cookbook' of Pipeline Recipes:** Add a `recipes/` directory to the repository with documented, ready-to-use `pipeline_config.yaml` files for common use cases (e.g., "FineWeb-style cleaning," "PII Redaction," "Deduplication").
4.  **Explore Dynamic Plugin Loading:** Investigate a plugin system (e.g., loading from `.so`/`.dll` files) that would allow users to compile and use their own custom Rust-based filters without needing to recompile the main TextBlaster binaries.
5.  **Implement Document-Level Aggregations:** Design a new "aggregator" mode or binary that can perform a second pass over the processed data to compute global statistics like TF-IDF scores or a complete dataset vocabulary.
6.  **Launch a Project Website:** Create a polished GitHub Pages or standalone website with comprehensive tutorials, the cookbook, benchmark results, and full API documentation to serve as the central hub for the user community.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs, feature requests, or improvements.

For contribution guidelines, please see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under:

*   Apache License, Version 2.0, ([LICENSE](LICENSE))
