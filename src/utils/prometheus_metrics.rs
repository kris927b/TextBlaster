// src/utils/prometheus_metrics.rs

use tracing::{error, info};

use crate::error::Result;
use axum::{http::StatusCode, routing::get, serve, Router};
use tokio::net::TcpListener; // Assuming Result is crate::error::Result for PipelineError

use once_cell::sync::Lazy;
use prometheus::{
    gather, register_counter, register_gauge, register_histogram, Counter, Encoder, Gauge,
    Histogram, TextEncoder,
};

// Metrics from Producer
pub static TASKS_PUBLISHED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_tasks_published_total",
        "Total number of tasks published."
    )
    .expect("Failed to register TASKS_PUBLISHED_TOTAL counter")
});

pub static TASK_PUBLISH_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_task_publish_errors_total",
        "Total number of errors during task publishing (serialization, broker ack)."
    )
    .expect("Failed to register TASK_PUBLISH_ERRORS_TOTAL counter")
});

pub static RESULTS_RECEIVED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_received_total",
        "Total number of results/outcomes received."
    )
    .expect("Failed to register RESULTS_RECEIVED_TOTAL counter")
});

pub static RESULTS_SUCCESS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_success_total",
        "Total number of successful results."
    )
    .expect("Failed to register RESULTS_SUCCESS_TOTAL counter")
});

pub static RESULTS_FILTERED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_filtered_total",
        "Total number of filtered results."
    )
    .expect("Failed to register RESULTS_FILTERED_TOTAL counter")
});

pub static RESULTS_ERROR_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_results_error_total",
        "Total number of error results."
    )
    .expect("Failed to register RESULTS_ERROR_TOTAL counter")
});

pub static RESULT_DESERIALIZATION_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "producer_result_deserialization_errors_total",
        "Total number of errors deserializing results."
    )
    .expect("Failed to register RESULT_DESERIALIZATION_ERRORS_TOTAL counter")
});

pub static ACTIVE_TASKS_IN_FLIGHT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "producer_active_tasks_in_flight",
        "Number of tasks published but not yet resolved."
    )
    .expect("Failed to register ACTIVE_TASKS_IN_FLIGHT gauge")
});

pub static TASK_PUBLISHING_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "producer_task_publishing_duration_seconds",
        "Histogram of task publishing latencies (from send to broker ack)."
    )
    .expect("Failed to register TASK_PUBLISHING_DURATION_SECONDS histogram")
});

// Metrics from Worker
pub static TASKS_PROCESSED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "worker_tasks_processed_total",
        "Total number of tasks processed by the worker."
    )
    .expect("Failed to register worker_tasks_processed_total counter")
});

pub static TASKS_FILTERED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "worker_tasks_filtered_total",
        "Total number of tasks filtered by the pipeline."
    )
    .expect("Failed to register worker_tasks_filtered_total counter")
});

pub static TASKS_FAILED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "worker_tasks_failed_total",
        "Total number of tasks that resulted in a pipeline error."
    )
    .expect("Failed to register worker_tasks_failed_total counter")
});

pub static TASK_DESERIALIZATION_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "worker_task_deserialization_errors_total",
        "Total number of errors deserializing incoming task messages."
    )
    .expect("Failed to register worker_task_deserialization_errors_total counter")
});

pub static OUTCOME_PUBLISH_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "worker_outcome_publish_errors_total",
        "Total number of errors publishing outcome messages."
    )
    .expect("Failed to register worker_outcome_publish_errors_total counter")
});

pub static TASK_PROCESSING_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "worker_task_processing_duration_seconds",
        "Histogram of task processing durations (from message receipt to outcome published/error)."
    )
    .expect("Failed to register worker_task_processing_duration_seconds histogram")
});

pub static ACTIVE_PROCESSING_TASKS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "worker_active_processing_tasks",
        "Number of tasks currently being processed concurrently."
    )
    .expect("Failed to register worker_active_processing_tasks gauge")
});

// Axum handler for /metrics
// This function needs to be async as it's used as an Axum handler.
// It's kept non-public as it's an internal detail of setup_prometheus_metrics.
async fn metrics_handler() -> (StatusCode, String) {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    if let Err(e) = encoder.encode(&gather(), &mut buffer) {
        error!("Could not encode prometheus metrics: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not encode prometheus metrics: {}", e),
        );
    }
    match String::from_utf8(buffer) {
        Ok(s) => (StatusCode::OK, s),
        Err(e) => {
            error!("Prometheus metrics UTF-8 error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Prometheus metrics UTF-8 error: {}", e),
            )
        }
    }
}

// Function to setup Prometheus metrics endpoint
// This function is now public to be used by binaries.
pub async fn setup_prometheus_metrics(metrics_port: Option<u16>) -> Result<()> {
    if let Some(port) = metrics_port {
        let app = Router::new().route("/metrics", get(metrics_handler));
        let listener_addr = format!("0.0.0.0:{}", port);
        info!(
            "Metrics endpoint will be available at http://{}/metrics",
            listener_addr
        );

        tokio::spawn(async move {
            match TcpListener::bind(&listener_addr).await {
                Ok(listener) => {
                    if let Err(e) = serve(listener, app).await {
                        error!("Metrics server error: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to bind metrics server to {}: {}", listener_addr, e);
                    // This error is within a spawned task, so we can't directly bubble it up
                    // with `?` from setup_prometheus_metrics. Logging is appropriate.
                    // Consider if this should return an error that prevents server startup.
                }
            }
        });
        Ok(())
    } else {
        info!("Prometheus metrics endpoint not configured (no port specified).");
        Ok(())
    }
}
