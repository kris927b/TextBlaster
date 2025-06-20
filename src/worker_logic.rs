// src/worker_logic.rs

use crate::config::pipeline::{PipelineConfig, StepConfig};
use crate::error::{PipelineError, Result}; // Use the library's Result type
use crate::executor::{PipelineExecutor, ProcessingStep};
use crate::utils::common::setup_channels_and_queues;
use futures::StreamExt; // For processing the consumer stream
use lapin::{Channel, Connection};
// Import necessary filters (adjust if steps change)
// {{ Remove ArrowNativeType import if no longer needed directly here }}
// use arrow::datatypes::ArrowNativeType;
use crate::config::worker::Args;
use crate::pipeline::filters::{
    C4BadWordsFilter,
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter, // Updated import
    GopherRepetitionFilter,
    LanguageDetectionFilter,
};
use crate::pipeline::token::TokenCounter;

use std::sync::Arc; // To share the executor across potential concurrent tasks
                    // {{ Add serde_json for result serialization }}
use tracing::{debug, error, info, info_span, instrument, warn}; // Added tracing

use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicPublishOptions},
    protocol::basic::AMQPProperties,
};

use crate::data_model::{ProcessingOutcome, TextDocument};
use crate::utils::prometheus_metrics::*;

// {{ Add the new function to build pipeline from configuration }}
/// Builds the processing pipeline based on the configuration read from YAML.
#[instrument(skip(config), fields(num_steps = config.pipeline.len()))]
pub fn build_pipeline_from_config(config: &PipelineConfig) -> Result<Vec<Box<dyn ProcessingStep>>> {
    let mut steps: Vec<Box<dyn ProcessingStep>> = Vec::new();
    info!("Building pipeline from configuration...");

    for (i, step_config) in config.pipeline.iter().enumerate() {
        let step_span = info_span!("pipeline_step", index = i, type = step_config.name());
        let _enter = step_span.enter();

        let step: Box<dyn ProcessingStep> = match step_config {
            StepConfig::C4QualityFilter(params) => {
                debug!(params = ?params, "Adding C4QualityFilter");
                Box::new(C4QualityFilter::new(
                    params.split_paragraph,
                    params.remove_citations,
                    params.filter_no_terminal_punct,
                    params.min_num_sentences,
                    params.min_words_per_line,
                    params.max_word_length,
                    params.filter_lorem_ipsum,
                    params.filter_javascript,
                    params.filter_curly_bracket,
                    params.filter_policy,
                ))
            }
            StepConfig::GopherRepetitionFilter(params) => {
                debug!(params = ?params, "Adding GopherRepetitionFilter");
                Box::new(GopherRepetitionFilter::new(
                    params.dup_line_frac,
                    params.dup_para_frac,
                    params.dup_line_char_frac,
                    params.dup_para_char_frac,
                    params.top_n_grams.clone(), // Clone the vec
                    params.dup_n_grams.clone(), // Clone the vec
                ))
            }
            StepConfig::GopherQualityFilter(params) => {
                debug!(params = ?params, "Adding GopherQualityFilter");
                Box::new(GopherQualityFilter::new(
                    params.min_doc_words,
                    params.max_doc_words,
                    params.min_avg_word_length,
                    params.max_avg_word_length,
                    params.max_symbol_word_ratio,
                    params.max_bullet_lines_ratio,
                    params.max_ellipsis_lines_ratio,
                    params.max_non_alpha_words_ratio,
                    params.min_stop_words,
                    params.stop_words.clone(), // Clone the Option<Vec<String>>
                ))
            }
            StepConfig::LanguageDetectionFilter(params) => {
                debug!(params = ?params, "Adding LanguageDetectionFilter");
                Box::new(LanguageDetectionFilter::new(
                    params.min_confidence,
                    params.allowed_languages.clone(),
                ))
            }
            StepConfig::C4BadWordsFilter(params) => {
                debug!(params = ?params, "Adding C4BadWordsFilter");
                Box::new(C4BadWordsFilter::new(params.clone()))
            }
            StepConfig::FineWebQualityFilter(params) => {
                // Updated variant name
                debug!(params = ?params, "Adding FineWebQualityFilter");

                Box::new(FineWebQualityFilter::new(
                    params.line_punct_thr,
                    params.line_punct_exclude_zero,
                    params.short_line_thr,
                    params.short_line_length,
                    params.char_duplicates_ratio,
                    params.new_line_ratio,
                    // params.language.clone(),
                    params.stop_chars.clone(),
                ))
            }
            StepConfig::TokenCounter(params) => {
                debug!(params=?params, "Adding TokenCounter");
                let token_step = TokenCounter::new(&params.tokenizer_name);
                if let Err(e) = token_step {
                    panic!("{}", e);
                }
                Box::new(token_step.unwrap())
            }
        };
        steps.push(step);
        info!("Added step: {}", step_config.name());
    }

    if steps.is_empty() {
        warn!("Warning: Building an empty pipeline from configuration!");
    } else {
        info!("Pipeline built successfully with {} steps.", steps.len());
    }
    Ok(steps)
}

/// Decoupled core processing logic for a single task.
///
/// Returns `Some(ProcessingOutcome)` if the document was successfully processed
/// or filtered. Returns `None` if there was an unrecoverable error.
pub async fn execute_processing_pipeline(
    data: &[u8],
    executor: Arc<PipelineExecutor>,
) -> Option<ProcessingOutcome> {
    ACTIVE_PROCESSING_TASKS.inc();
    let processing_timer = TASK_PROCESSING_DURATION_SECONDS.start_timer();

    let result = match serde_json::from_slice::<TextDocument>(data) {
        Ok(doc) => {
            let original_doc_id = doc.id.clone();
            let task_span = info_span!("process_task", doc_id = %original_doc_id);
            let _enter = task_span.enter();
            debug!("Processing document");

            match executor.run_single_async(doc.clone()).await {
                Ok(processed_doc) => {
                    debug!(processed_doc_id = %processed_doc.id, "Successfully processed document");
                    TASKS_PROCESSED_TOTAL.inc();
                    Some(ProcessingOutcome::Success(processed_doc))
                }
                Err(PipelineError::StepError { step_name, source }) => match *source {
                    PipelineError::DocumentFiltered { document, reason } => {
                        debug!(filtered_doc_id = %document.id, %step_name, %reason, "Document was filtered");
                        TASKS_FILTERED_TOTAL.inc();
                        Some(ProcessingOutcome::Filtered {
                            document: *document,
                            reason,
                        })
                    }
                    other => {
                        error!(%step_name, error = %other, "Pipeline step failed");
                        TASKS_FAILED_TOTAL.inc();
                        None
                    }
                },
                Err(e) => {
                    error!(error = %e, "Unexpected pipeline error");
                    TASKS_FAILED_TOTAL.inc();
                    None
                }
            }
        }
        Err(e) => {
            error!(error = %e, payload = %String::from_utf8_lossy(data), "Failed to deserialize task message");
            TASK_DESERIALIZATION_ERRORS_TOTAL.inc();
            None
        }
    };

    processing_timer.observe_duration();
    ACTIVE_PROCESSING_TASKS.dec();

    result
}

pub async fn process_single_task(
    delivery: Delivery,
    executor: Arc<PipelineExecutor>,
    publish_channel: Channel,
    results_queue_name: &str,
) {
    ACTIVE_PROCESSING_TASKS.inc();
    let processing_timer = TASK_PROCESSING_DURATION_SECONDS.start_timer();

    let result: Option<ProcessingOutcome> =
        execute_processing_pipeline(&delivery.data, executor).await;

    if let Some(outcome) = result {
        match serde_json::to_vec(&outcome) {
            Ok(payload) => {
                match publish_channel
                    .basic_publish(
                        "",
                        results_queue_name,
                        BasicPublishOptions::default(),
                        &payload,
                        AMQPProperties::default().with_delivery_mode(2),
                    )
                    .await
                {
                    Ok(_) => debug!("Published outcome"),
                    Err(e) => {
                        error!(error = %e, "Failed publish confirmation for outcome");
                        OUTCOME_PUBLISH_ERRORS_TOTAL.inc();
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to serialize outcome");
            }
        }
    }

    if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
        error!(error = %ack_err, "Failed to ack task message");
    }

    processing_timer.observe_duration();
    ACTIVE_PROCESSING_TASKS.dec();
}

pub async fn process_tasks_with_executor(
    args: &Args,
    conn: &Connection,
    executor: Arc<PipelineExecutor>,
) -> Result<()> {
    let (publish_channel, consumer) = setup_channels_and_queues(
        conn,
        &args.results_queue,
        &args.task_queue,
        args.prefetch_count,
        "worker".to_string(),
    )
    .await
    .unwrap();

    let mut consumer = consumer;

    while let Some(delivery_result) = consumer.next().await {
        if let Ok(delivery) = delivery_result {
            let executor_clone = Arc::clone(&executor);
            let publish_channel_clone = publish_channel.clone();
            let results_queue_name = args.results_queue.clone();

            tokio::spawn(async move {
                process_single_task(
                    delivery,
                    executor_clone,
                    publish_channel_clone,
                    &results_queue_name,
                )
                .await;
            });
        } else {
            error!("Error receiving task message. Worker stopping.");
            return Err(PipelineError::QueueError(
                "Consumer stream error".to_string(),
            ));
        }
    }

    info!("Consumer stream ended.");
    Ok(())
}
