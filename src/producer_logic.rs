// src/producer_logic.rs
use crate::config::parquet::ParquetInputConfig;
use crate::config::producer::Args;
use crate::data_model::ProcessingOutcome; // Still needed for aggregate_results
use crate::data_model::TextDocument; // TextDocument is needed for aggregate_results
use crate::error::{PipelineError, Result as AppResult};
use crate::pipeline::readers::ParquetReader;
use crate::pipeline::writers::parquet_writer::ParquetWriter;
use crate::utils::prometheus_metrics::*;
use async_trait::async_trait;
use chrono::Utc;
use futures::{pin_mut, Stream, StreamExt};
use indicatif::ProgressBar;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions,
        QueueDeclareOptions,
    },
    protocol::basic::AMQPProperties,
    publisher_confirm::Confirmation,
    types::FieldTable,
    Channel as LapinChannel, // Alias lapin::Channel to avoid confusion
    Consumer,
    Result as LapinResult,
};
use serde_json;
use std::time::Instant;
use tracing::{error, info, info_span, warn}; // For aggregate_results consumer tag

pub const PARQUET_WRITE_BATCH_SIZE: usize = 500;

#[async_trait]
pub trait TaskPublisherChannel: Send + Sync {
    async fn queue_declare(
        &self,
        name: &str,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> LapinResult<()>;
    async fn basic_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        options: BasicPublishOptions,
        payload: &[u8],
        properties: AMQPProperties,
    ) -> LapinResult<Confirmation>;
    async fn confirm_select(&self, options: ConfirmSelectOptions) -> LapinResult<()>;
}

#[async_trait]
impl TaskPublisherChannel for LapinChannel {
    async fn queue_declare(
        &self,
        name: &str,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> LapinResult<()> {
        LapinChannel::queue_declare(self, name, options, arguments).await?;
        Ok(())
    }
    async fn basic_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        options: BasicPublishOptions,
        payload: &[u8],
        properties: AMQPProperties,
    ) -> LapinResult<Confirmation> {
        let publisher_confirmation =
            LapinChannel::basic_publish(self, exchange, routing_key, options, payload, properties)
                .await?;
        publisher_confirmation.await
    }
    async fn confirm_select(&self, options: ConfirmSelectOptions) -> LapinResult<()> {
        LapinChannel::confirm_select(self, options).await
    }
}

pub async fn publish_tasks<CH: TaskPublisherChannel + ?Sized>(
    args: &Args,
    publish_channel: &CH,
    publishing_pb: &ProgressBar,
) -> AppResult<u64> {
    publish_channel
        .queue_declare(
            &args.task_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    info!("Declared durable task queue '{}'", args.task_queue);

    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone(),
        batch_size: Some(1024),
    };
    let reader = ParquetReader::new(parquet_config);

    info!("Reading documents and publishing tasks...");
    let mut published_count = 0u64;
    let mut read_errors = 0u64;
    let doc_iterator = reader.read_documents()?;

    let publish_start_time = Instant::now();

    for doc_result in doc_iterator {
        publishing_pb.tick();
        match doc_result {
            Ok(doc) => {
                let _doc_span = info_span!("publishing_doc", doc_id = %doc.id).entered();
                match serde_json::to_vec(&doc) {
                    Ok(payload) => {
                        info!("Serialized document for publishing.");
                        let task_publish_timer = TASK_PUBLISHING_DURATION_SECONDS.start_timer();
                        let confirmation = publish_channel
                            .basic_publish(
                                "",
                                &args.task_queue,
                                BasicPublishOptions::default(),
                                &payload,
                                AMQPProperties::default().with_delivery_mode(2),
                            )
                            .await?;
                        task_publish_timer.observe_duration();

                        match confirmation {
                            Confirmation::Ack(_) | Confirmation::NotRequested => {
                                published_count += 1;
                                TASKS_PUBLISHED_TOTAL.inc();
                                ACTIVE_TASKS_IN_FLIGHT.inc();
                                publishing_pb.inc(1);
                                if matches!(confirmation, Confirmation::Ack(_)) {
                                    info!("Successfully published task and received ACK.");
                                } else {
                                    info!("Successfully published task (no confirmation requested/received).");
                                }
                            }
                            Confirmation::Nack(_) => {
                                TASK_PUBLISH_ERRORS_TOTAL.inc();
                                error!(doc_id = %doc.id, "FATAL: Broker NACKed task. Stopping.");
                                return Err(PipelineError::QueueError(format!(
                                    "Publish confirmation failed (NACK) for doc {}",
                                    doc.id
                                )));
                            }
                        }
                    }
                    Err(e) => {
                        TASK_PUBLISH_ERRORS_TOTAL.inc();
                        warn!(doc_id = %doc.id, error = %e, "Failed to serialize task. Skipping.");
                        read_errors += 1;
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading document for task. Skipping.");
                read_errors += 1;
            }
        }
    }
    let publishing_duration = publish_start_time.elapsed();
    publishing_pb.finish_with_message(format!(
        "Finished publishing {} tasks in {}. Read/Serialization Errors: {}",
        published_count,
        indicatif::HumanDuration(publishing_duration),
        read_errors
    ));
    Ok(published_count)
}

#[async_trait]
pub trait ResultConsumerChannel: Send + Sync {
    async fn queue_declare(
        &self,
        name: &str,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> LapinResult<()>;
    async fn basic_consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        options: BasicConsumeOptions,
        arguments: FieldTable,
    ) -> LapinResult<Consumer>;
}

#[async_trait]
impl ResultConsumerChannel for LapinChannel {
    async fn queue_declare(
        &self,
        name: &str,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> LapinResult<()> {
        let _ = LapinChannel::queue_declare(self, name, options, arguments).await;
        Ok(())
    }
    async fn basic_consume(
        &self,
        queue: &str,
        consumer_tag: &str,
        options: BasicConsumeOptions,
        arguments: FieldTable,
    ) -> LapinResult<Consumer> {
        LapinChannel::basic_consume(self, queue, consumer_tag, options, arguments).await
    }
}

pub async fn aggregate_results_from_stream<S>(
    args: &Args,
    stream: S,
    published_count: u64,
    aggregation_pb: &ProgressBar,
) -> AppResult<(u64, u64, u64)>
where
    S: Stream<Item = ProcessingOutcome>,
{
    pin_mut!(stream);
    if let Some(parent_dir) = std::path::Path::new(&args.output_file).parent() {
        tokio::fs::create_dir_all(parent_dir)
            .await
            .map_err(|e| PipelineError::IoError { source: e })?;
    }
    if let Some(parent_dir) = std::path::Path::new(&args.excluded_file).parent() {
        tokio::fs::create_dir_all(parent_dir)
            .await
            .map_err(|e| PipelineError::IoError { source: e })?;
    }

    let mut parquet_writer_output = ParquetWriter::new(&args.output_file)?;
    let mut parquet_writer_excluded = ParquetWriter::new(&args.excluded_file)?;

    let mut results_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut excluded_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut outcomes_received_count = 0u64;
    let mut success_count = 0u64;
    let mut filtered_count = 0u64;

    let aggregation_start_time = Instant::now();

    while outcomes_received_count < published_count {
        aggregation_pb.tick();
        if let Some(outcome) = stream.next().await {
            outcomes_received_count += 1;
            aggregation_pb.inc(1);
            ACTIVE_TASKS_IN_FLIGHT.dec();

            match outcome {
                ProcessingOutcome::Success(doc) => {
                    success_count += 1;
                    results_batch.push(doc);
                    if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                        parquet_writer_output.write_batch(&results_batch)?;
                        results_batch.clear();
                    }
                }
                ProcessingOutcome::Filtered {
                    document,
                    reason: _,
                } => {
                    filtered_count += 1;
                    excluded_batch.push(document);
                    if excluded_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                        parquet_writer_excluded.write_batch(&excluded_batch)?;
                        excluded_batch.clear();
                    }
                }
                ProcessingOutcome::Error { .. } => {
                    // Count/metrics can go here if needed
                }
            }
        } else {
            warn!("Outcome stream closed before all outcomes received.");
            break;
        }
    }

    aggregation_pb.finish_with_message(format!(
        "Finished consuming (Received {}/{}) in {}.",
        outcomes_received_count,
        published_count,
        indicatif::HumanDuration(aggregation_start_time.elapsed())
    ));

    if !results_batch.is_empty() {
        parquet_writer_output.write_batch(&results_batch)?;
    }
    if !excluded_batch.is_empty() {
        parquet_writer_excluded.write_batch(&excluded_batch)?;
    }

    parquet_writer_output.close()?;
    parquet_writer_excluded.close()?;

    Ok((outcomes_received_count, success_count, filtered_count))
}

pub async fn aggregate_results<CH: ResultConsumerChannel + ?Sized>(
    args: &Args,
    consume_channel: &CH,
    published_count: u64,
    aggregation_pb: &ProgressBar,
) -> AppResult<(u64, u64, u64)> {
    info!("Starting results aggregation phase...");

    consume_channel
        .queue_declare(
            &args.results_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    let consumer_tag = format!(
        "producer-aggregator-{}-{}",
        std::process::id(),
        Utc::now().timestamp()
    );

    let consumer = consume_channel
        .basic_consume(
            &args.results_queue,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    // Map deliveries into ProcessingOutcome values
    let mapped_stream = consumer.filter_map(|delivery_result| async {
        match delivery_result {
            Ok(delivery) => match serde_json::from_slice::<ProcessingOutcome>(&delivery.data) {
                Ok(outcome) => {
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                    Some(outcome)
                }
                Err(err) => {
                    warn!(error = %err, "Failed to deserialize outcome.");
                    None
                }
            },
            Err(err) => {
                error!(error = %err, "Failed to receive delivery.");
                None
            }
        }
    });

    aggregate_results_from_stream(args, mapped_stream, published_count, aggregation_pb).await
}
