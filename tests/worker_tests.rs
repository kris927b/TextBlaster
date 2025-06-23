// tests/worker_logic_tests.rs

use std::collections::HashMap;
use std::sync::Arc;

use axum::async_trait;
use TextBlaster::data_model::{ProcessingOutcome, TextDocument};
use TextBlaster::error::{PipelineError, Result};
use TextBlaster::executor::{PipelineExecutor, ProcessingStep};
use TextBlaster::worker_logic::execute_processing_pipeline;

struct FailingStep;

#[async_trait]
impl ProcessingStep for FailingStep {
    fn name(&self) -> &'static str {
        "FailingStep"
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        Err(PipelineError::StepError {
            step_name: "FailingStep".to_string(),
            source: Box::new(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason: "FailingStep".to_string(),
            }),
        })
    }
}

struct FilteringStep;

#[async_trait]
impl ProcessingStep for FilteringStep {
    fn name(&self) -> &'static str {
        "FilteringStep"
    }

    async fn process(&self, doc: TextDocument) -> Result<TextDocument> {
        Err(PipelineError::DocumentFiltered {
            document: Box::new(doc),
            reason: "Blocked by test filter".to_string(),
        })
    }
}

struct IdentityStep;

#[async_trait]
impl ProcessingStep for IdentityStep {
    fn name(&self) -> &'static str {
        "IdentityStep"
    }

    async fn process(&self, doc: TextDocument) -> Result<TextDocument> {
        Ok(doc)
    }
}

#[tokio::test]
async fn test_success_path() {
    let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(IdentityStep)];
    let executor = Arc::new(PipelineExecutor::new(steps));

    let input_doc = TextDocument {
        id: "doc-1".into(),
        content: "This is a test".into(),
        source: "test-suite".into(),
        metadata: HashMap::new(),
        ..Default::default()
    };

    let raw = serde_json::to_vec(&input_doc).unwrap();
    let outcome = execute_processing_pipeline(&raw, executor).await;

    match outcome.unwrap() {
        ProcessingOutcome::Success(doc) => assert_eq!(doc.id, "doc-1"),
        _ => panic!("Expected success outcome"),
    }
}

#[tokio::test]
async fn test_deserialization_failure() {
    let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(IdentityStep)];
    let executor = Arc::new(PipelineExecutor::new(steps));

    let raw = b"not valid json".to_vec();
    let result = execute_processing_pipeline(&raw, executor).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_filtered_outcome() {
    let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(FilteringStep)];
    let executor = Arc::new(PipelineExecutor::new(steps));

    let input_doc = TextDocument {
        id: "doc-filtered".into(),
        content: "Block me".into(),
        source: "filter-test".into(),
        metadata: HashMap::new(),
        ..Default::default()
    };

    let raw = serde_json::to_vec(&input_doc).unwrap();
    let outcome = execute_processing_pipeline(&raw, executor).await;

    match outcome.unwrap() {
        ProcessingOutcome::Filtered { document, reason } => {
            assert_eq!(document.id, "doc-filtered");
            assert_eq!(reason, "Blocked by test filter");
        }
        _ => panic!("Expected filtered outcome"),
    }
}

#[tokio::test]
async fn test_failing_step_returns_none() {
    let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(FailingStep)];
    let executor = Arc::new(PipelineExecutor::new(steps));

    let input_doc = TextDocument {
        id: "doc-fail".into(),
        content: "Cause failure".into(),
        source: "fail-test".into(),
        metadata: HashMap::new(),
        ..Default::default()
    };

    let raw = serde_json::to_vec(&input_doc).unwrap();
    let outcome = execute_processing_pipeline(&raw, executor).await;

    assert!(outcome.is_none());
}

#[tokio::test]
async fn test_chained_steps_success() {
    let steps: Vec<Box<dyn ProcessingStep>> = vec![
        Box::new(IdentityStep),
        Box::new(IdentityStep),
        Box::new(IdentityStep),
    ];
    let executor = Arc::new(PipelineExecutor::new(steps));

    let input_doc = TextDocument {
        id: "doc-chain".into(),
        content: "Chain of steps".into(),
        source: "chained".into(),
        metadata: HashMap::new(),
        ..Default::default()
    };

    let raw = serde_json::to_vec(&input_doc).unwrap();
    let outcome = execute_processing_pipeline(&raw, executor).await;

    match outcome.unwrap() {
        ProcessingOutcome::Success(doc) => assert_eq!(doc.id, "doc-chain"),
        _ => panic!("Expected success outcome"),
    }
}
