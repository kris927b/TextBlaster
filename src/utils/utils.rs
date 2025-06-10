// src/utils/utils.rs

use lapin::{Connection, ConnectionProperties, Result as LapinResult};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

use crate::error::{PipelineError, Result};
use axum::{http::StatusCode, routing::get, serve, Router};
use prometheus::{gather, Encoder, TextEncoder};
use tokio::net::TcpListener; // Assuming Result is crate::error::Result for PipelineError

// Helper function to connect to RabbitMQ with retry (already here)
pub async fn connect_rabbitmq(addr: &str) -> LapinResult<Connection> {
    let options = ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);

    let mut attempts = 0;
    loop {
        match Connection::connect(addr, options.clone()).await {
            Ok(conn) => {
                info!("Successfully connected to RabbitMQ at {}", addr);
                return Ok(conn);
            }
            Err(e) => {
                attempts += 1;
                error!(
                    attempt = attempts,
                    error = %e,
                    "Failed to connect to RabbitMQ. Retrying in 5 seconds..."
                );
                if attempts >= 5 {
                    return Err(e);
                }
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

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
