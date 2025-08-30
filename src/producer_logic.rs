use crate::config::parquet::ParquetInputConfig;
use crate::config::producer::Args;
use crate::data_model::ProcessingOutcome;
use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result as AppResult};
use crate::pipeline::readers::{BaseReader, ParquetReader};
use crate::pipeline::writers::{BaseWriter, ParquetWriter};
use crate::utils::common::{connect_rabbitmq, setup_channels_and_queues};
use crate::utils::prometheus_metrics::*;
use futures::{pin_mut, Stream, StreamExt};
use lapin::{
    options::{BasicAckOptions, BasicPublishOptions},
    protocol::basic::AMQPProperties,
    Channel, Consumer,
};
use serde_json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, info_span, warn};
use uuid::Uuid;

use crate::server::{AppState, JobStatus};

pub const PARQUET_WRITE_BATCH_SIZE: usize = 500;

pub async fn run_job_in_background(app_state: Arc<AppState>, args: Args, job_id: Uuid) {
    let job_result = execute_job(app_state.clone(), args, job_id).await;

    let mut job_store = app_state.job_store.write().await;
    let job = job_store.get_mut(&job_id).unwrap();

    match job_result {
        Ok(_) => {
            job.status = JobStatus::Completed;
        }
        Err(e) => {
            job.status = JobStatus::Failed(e.to_string());
        }
    }
}

async fn execute_job(app_state: Arc<AppState>, args: Args, job_id: Uuid) -> AppResult<()> {
    let conn = connect_rabbitmq(&args.amqp_addr).await?;
    let (publish_channel, consumer) =
        setup_channels_and_queues(&conn, &args.task_queue, &args.results_queue, args.prefetch_count, "producer".to_string()).await?;

    let (published_count_tx, mut published_count_rx) = mpsc::channel(1);

    let publisher_handle = tokio::spawn(publish_tasks_logic(
        args.clone(),
        publish_channel,
        published_count_tx,
    ));

    let published_count = published_count_rx.recv().await.unwrap_or(0);

    {
        let mut job_store = app_state.job_store.write().await;
        let job = job_store.get_mut(&job_id).unwrap();
        job.published_tasks = published_count;
    }

    let aggregator_handle = tokio::spawn(aggregate_results_logic(
        app_state.clone(),
        args,
        consumer,
        published_count,
        job_id,
    ));

    publisher_handle.await??;
    aggregator_handle.await??;

    Ok(())
}

async fn publish_tasks_logic(
    args: Args,
    publish_channel: Channel,
    published_count_tx: mpsc::Sender<u64>,
) -> AppResult<()> {
    let parquet_config = ParquetInputConfig {
        path: args.input_file.clone(),
        text_column: args.text_column.clone(),
        id_column: args.id_column.clone().expect("id_column to be defined"),
        batch_size: Some(1024),
    };
    let reader = ParquetReader::new(parquet_config)?;
    let doc_iterator = reader.read_documents()?;

    let mut published_count = 0u64;
    for doc_result in doc_iterator {
        match doc_result {
            Ok(doc) => {
                let payload = serde_json::to_vec(&doc)?;
                publish_channel
                    .basic_publish(
                        "",
                        &args.task_queue,
                        BasicPublishOptions::default(),
                        &payload,
                        AMQPProperties::default().with_delivery_mode(2),
                    )
                    .await?
                    .await?;
                published_count += 1;
            }
            Err(e) => {
                warn!(error = %e, "Error reading document for task. Skipping.");
            }
        }
    }

    published_count_tx.send(published_count).await.unwrap();
    Ok(())
}

async fn aggregate_results_logic(
    app_state: Arc<AppState>,
    args: Args,
    consumer: Consumer,
    published_count: u64,
    job_id: Uuid,
) -> AppResult<()> {
    let mapped_stream = consumer.filter_map(|delivery_result| async {
        match delivery_result {
            Ok(delivery) => {
                let _ = delivery.ack(BasicAckOptions::default()).await;
                match serde_json::from_slice::<ProcessingOutcome>(&delivery.data) {
                    Ok(outcome) => Some(outcome),
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

    aggregate_results_from_stream_logic(
        app_state,
        args,
        mapped_stream,
        published_count,
        job_id,
    )
    .await?;

    Ok(())
}

async fn aggregate_results_from_stream_logic<S>(
    app_state: Arc<AppState>,
    args: Args,
    stream: S,
    published_count: u64,
    job_id: Uuid,
) -> AppResult<()>
where
    S: Stream<Item = ProcessingOutcome> + Unpin,
{
    pin_mut!(stream);
    if let Some(parent_dir) = std::path::Path::new(&args.output_file).parent() {
        tokio::fs::create_dir_all(parent_dir).await?;
    }
    if let Some(parent_dir) = std::path::Path::new(&args.excluded_file).parent() {
        tokio::fs::create_dir_all(parent_dir).await?;
    }

    let mut parquet_writer_output = ParquetWriter::new(&args.output_file)?;
    let mut parquet_writer_excluded = ParquetWriter::new(&args.excluded_file)?;

    let mut results_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut excluded_batch: Vec<TextDocument> = Vec::with_capacity(PARQUET_WRITE_BATCH_SIZE);
    let mut outcomes_received_count = 0u64;

    while outcomes_received_count < published_count {
        if let Some(outcome) = stream.next().await {
            outcomes_received_count += 1;
            {
                let mut job_store = app_state.job_store.write().await;
                let job = job_store.get_mut(&job_id).unwrap();
                job.aggregated_results = outcomes_received_count;
            }

            match outcome {
                ProcessingOutcome::Success(doc) => {
                    results_batch.push(doc);
                    if results_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                        parquet_writer_output.write_batch(&results_batch)?;
                        results_batch.clear();
                    }
                }
                ProcessingOutcome::Filtered { document, .. } => {
                    excluded_batch.push(document);
                    if excluded_batch.len() >= PARQUET_WRITE_BATCH_SIZE {
                        parquet_writer_excluded.write_batch(&excluded_batch)?;
                        excluded_batch.clear();
                    }
                }
                ProcessingOutcome::Error { .. } => {}
            }
        } else {
            warn!("Outcome stream closed before all outcomes received.");
            break;
        }
    }

    if !results_batch.is_empty() {
        parquet_writer_output.write_batch(&results_batch)?;
    }
    if !excluded_batch.is_empty() {
        parquet_writer_excluded.write_batch(&excluded_batch)?;
    }

    parquet_writer_output.close()?;
    parquet_writer_excluded.close()?;

    Ok(())
}
