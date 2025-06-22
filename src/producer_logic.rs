// src/producer_logic.rs
use crate::config::parquet::ParquetInputConfig;
use crate::config::producer::Args;
use crate::data_model::ProcessingOutcome; // Still needed for aggregate_results
use crate::data_model::TextDocument; // TextDocument is needed for aggregate_results
use crate::error::{PipelineError, Result as AppResult};
use crate::pipeline::readers::{BaseReader, ParquetReader};
use crate::pipeline::writers::{BaseWriter, ParquetWriter};
use crate::utils::prometheus_metrics::*;
use futures::{pin_mut, Stream, StreamExt};
use indicatif::ProgressBar;
use lapin::{
    options::{BasicAckOptions, BasicPublishOptions},
    protocol::basic::AMQPProperties,
    Channel, Consumer,
};
use serde_json;
use std::time::Instant;
use tracing::{error, info, info_span, warn}; // For aggregate_results consumer tag

pub const PARQUET_WRITE_BATCH_SIZE: usize = 500;

pub async fn publish_tasks(
    args: &Args,
    publish_channel: &Channel,
    publishing_pb: &ProgressBar,
) -> AppResult<u64> {
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
                            .await;
                        task_publish_timer.observe_duration();

                        match confirmation {
                            Ok(_) => {
                                published_count += 1;
                                TASKS_PUBLISHED_TOTAL.inc();
                                ACTIVE_TASKS_IN_FLIGHT.inc();
                                publishing_pb.inc(1);
                                info!("Successfully published task");
                            }
                            Err(e) => {
                                TASK_PUBLISH_ERRORS_TOTAL.inc();
                                error!(doc_id = %doc.id, "FATAL: Broker NACKed task. Stopping.");
                                return Err(PipelineError::QueueError(format!(
                                    "Publish confirmation failed (NACK) for doc {}. Error {:?}",
                                    doc.id, e
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

pub async fn aggregate_results(
    args: &Args,
    consumer: Consumer,
    published_count: u64,
    aggregation_pb: &ProgressBar,
) -> AppResult<(u64, u64, u64)> {
    info!("Starting results aggregation phase...");
    // Map deliveries into ProcessingOutcome values
    let mapped_stream = consumer.filter_map(|delivery_result| async {
        match delivery_result {
            Ok(delivery) => {
                // let s = std::str::from_utf8(&delivery.data).unwrap();
                // warn!("{}", s);
                match serde_json::from_slice::<ProcessingOutcome>(&delivery.data) {
                    Ok(outcome) => {
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                        Some(outcome)
                    }
                    Err(err) => {
                        warn!(error = %err, "Failed to deserialize outcome.");
                        None
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to receive delivery.");
                None
            }
        }
    });

    aggregate_results_from_stream(args, mapped_stream, published_count, aggregation_pb).await
}
