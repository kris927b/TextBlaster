// src/utils/utils.rs

use lapin::{Connection, ConnectionProperties, Result as LapinResult};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

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
