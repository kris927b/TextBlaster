// src/utils/utils.rs

use lapin::{
    options::{BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer, Result as LapinResult,
};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

use crate::error::{PipelineError, Result};

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

pub async fn setup_channels_and_queues(
    conn: &Connection,
    publish_queue: &str,
    consume_queue: &str,
    prefetch_count: u16,
    binary: String, // For now just string, but could be enum of worker | producer
) -> Result<(Channel, Consumer)> {
    let consume_channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!(
            "{} failed to create consume channel: {}",
            binary, e
        ))
    })?;
    let publish_channel = conn.create_channel().await.map_err(|e| {
        PipelineError::QueueError(format!(
            "{} failed to create publish channel: {}",
            binary, e
        ))
    })?;

    publish_channel
        .queue_declare(
            publish_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            PipelineError::QueueError(format!("{} failed to declare task queue: {}", binary, e))
        })?;

    consume_channel
        .queue_declare(
            consume_queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| {
            PipelineError::QueueError(format!("{} failed to declare results queue: {}", binary, e))
        })?;

    consume_channel
        .basic_qos(prefetch_count, BasicQosOptions::default())
        .await
        .map_err(|e| PipelineError::QueueError(format!("Failed to set QoS: {}", e)))?;

    let consumer_tag = format!(
        "{}-{}-{}",
        binary,
        std::process::id(),
        chrono::Utc::now().timestamp()
    );
    let consumer = consume_channel
        .basic_consume(
            consume_queue,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok((publish_channel, consumer))
}
