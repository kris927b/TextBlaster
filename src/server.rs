use crate::config::producer::Args;
use crate::producer_logic::run_job_in_background;
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use tower_http::services::ServeFile;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use uuid::Uuid;

// Represents the state of a job
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum JobStatus {
    Pending,
    InProgress,
    Completed,
    Failed(String),
}

// Represents a job in the system
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub status: JobStatus,
    pub published_tasks: u64,
    pub aggregated_results: u64,
    pub output_file: Option<String>,
    pub excluded_file: Option<String>,
}

impl Job {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            status: JobStatus::Pending,
            published_tasks: 0,
            aggregated_results: 0,
            output_file: None,
            excluded_file: None,
        }
    }
}

// The application state, shared across all handlers
#[derive(Clone)]
pub struct AppState {
    pub job_store: std::sync::Arc<RwLock<HashMap<Uuid, Job>>>,
    pub temp_storage_path: String,
}

async fn upload_handler(
    State(app_state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let file_id = Uuid::new_v4();
    let file_path = format!("{}/{}.parquet", app_state.temp_storage_path, file_id);

    while let Some(field) = multipart.next_field().await.unwrap() {
        let _name = field.name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(&data).await.unwrap();
    }

    Json(json!({ "file_id": file_id.to_string() }))
}

#[derive(Deserialize)]
pub struct SubmitRequest {
    pub file_id: Uuid,
    pub amqp_addr: String,
    pub task_queue: String,
    pub results_queue: String,
    pub prefetch_count: u16,
    pub text_column: String,
    pub id_column: String,
}

async fn submit_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<SubmitRequest>,
) -> impl IntoResponse {
    let job_id = Uuid::new_v4();
    let mut job = Job::new();
    job.id = job_id;
    job.status = JobStatus::Pending;

    app_state.job_store.write().await.insert(job_id, job.clone());

    let input_file = format!("{}/{}.parquet", app_state.temp_storage_path, payload.file_id);
    let output_file = format!("{}/{}_output.parquet", app_state.temp_storage_path, job_id);
    let excluded_file = format!("{}/{}_excluded.parquet", app_state.temp_storage_path, job_id);

    let args = Args {
        input_file,
        output_file: output_file.clone(),
        excluded_file: excluded_file.clone(),
        amqp_addr: payload.amqp_addr,
        task_queue: payload.task_queue,
        results_queue: payload.results_queue,
        prefetch_count: payload.prefetch_count,
        text_column: payload.text_column,
        id_column: Some(payload.id_column),
        metrics_port: 9090, // This should probably be configured elsewhere
    };

    tokio::spawn(run_job_in_background(app_state.clone(), args, job_id));

    Json(json!({ "job_id": job_id.to_string() }))
}

async fn status_handler(
    State(app_state): State<Arc<AppState>>,
    Path(job_id): Path<Uuid>,
) -> impl IntoResponse {
    let job_store = app_state.job_store.read().await;
    if let Some(job) = job_store.get(&job_id) {
        (StatusCode::OK, Json(job.clone())).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Job not found").into_response()
    }
}

async fn download_handler(
    State(app_state): State<Arc<AppState>>,
    Path((job_id, file_type)): Path<(Uuid, String)>,
) -> impl IntoResponse {
    let job_store = app_state.job_store.read().await;
    if let Some(job) = job_store.get(&job_id) {
        if job.status == JobStatus::Completed {
            let file_path = if file_type == "output" {
                job.output_file.clone()
            } else if file_type == "excluded" {
                job.excluded_file.clone()
            } else {
                None
            };

            if let Some(path) = file_path {
                return ServeFile::new(path).into_response();
            }
        }
    }
    (StatusCode::NOT_FOUND, "File not found").into_response()
}


// The main function to run the server
pub async fn run_server() {
    let app_state = Arc::new(AppState {
        job_store: std::sync::Arc::new(RwLock::new(HashMap::new())),
        temp_storage_path: tempfile::tempdir()
            .expect("Failed to create temp dir")
            .into_path()
            .to_str()
            .expect("Failed to convert path to string")
            .to_string(),
    });

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/upload", post(upload_handler))
        .route("/submit", post(submit_handler))
        .route("/status/:job_id", get(status_handler))
        .route("/download/:job_id/:file_type", get(download_handler))
        .with_state(app_state)
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)); // 1 GB limit

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
