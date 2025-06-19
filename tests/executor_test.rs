use async_trait::async_trait;
use std::collections::HashMap;
use TextBlaster::data_model::TextDocument;
use TextBlaster::error::{PipelineError, Result};
use TextBlaster::executor::{PipelineExecutor, ProcessingStep}; // Assuming Result is crate::error::Result

// Helper function to create a TextDocument for testing
fn create_test_document(id: &str, content: &str) -> TextDocument {
    TextDocument {
        id: id.to_string(),
        source: "test_source".to_string(),
        content: content.to_string(),
        metadata: HashMap::new(),
    }
}

// Mock ProcessingStep
struct MockProcessingStep {
    name: &'static str,
    process_fn: Box<dyn Fn(TextDocument) -> Result<TextDocument> + Send + Sync + 'static>,
    should_error: bool,
    error_message: Option<String>,
}

impl MockProcessingStep {
    fn new<F>(name: &'static str, process_fn: F) -> Self
    where
        F: Fn(TextDocument) -> Result<TextDocument> + Send + Sync + 'static,
    {
        MockProcessingStep {
            name,
            process_fn: Box::new(process_fn),
            should_error: false,
            error_message: None,
        }
    }

    #[allow(dead_code)] // To allow should_error and error_message to be unused in some tests
    fn new_error_step(name: &'static str, error_message: &str) -> Self {
        let error_message_owned = error_message.to_string(); // Create one owned string
        MockProcessingStep {
            name,
            process_fn: Box::new(move |_doc| {
                Err(PipelineError::IoError {
                    source: std::io::Error::new(
                        std::io::ErrorKind::Other,
                        error_message_owned.clone(), // Clone for the closure
                    ),
                })
            }),
            should_error: true,
            error_message: Some(error_message.to_string()), // Use the original owned string for the field
        }
    }
}

#[async_trait]
impl ProcessingStep for MockProcessingStep {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        if self.should_error {
            Err(PipelineError::IoError {
                source: std::io::Error::new(
                    std::io::ErrorKind::Other,
                    self.error_message
                        .clone()
                        .unwrap_or_else(|| "Mock error".to_string()),
                ),
            })
        } else {
            (self.process_fn)(document)
        }
    }
}

// Basic pass-through step
fn mock_step_passthrough(doc: TextDocument) -> Result<TextDocument> {
    Ok(doc)
}

// Step that modifies content
fn mock_step_modify_content(mut doc: TextDocument) -> Result<TextDocument> {
    doc.content = format!("{} processed by {}", doc.content, "modify_content_step");
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*; // Import items from outer module

    #[test]
    fn test_new_executor_with_empty_steps() {
        let steps: Vec<Box<dyn ProcessingStep>> = vec![];
        let executor = PipelineExecutor::new(steps);
        // Simply creating it without panic is a pass for this case.
        // The internal steps vector is private, so we cannot assert its length directly.
        // Successful creation is sufficient for this test.
    }

    #[test]
    fn test_new_executor_with_steps() {
        let step1 = MockProcessingStep::new("step1", mock_step_passthrough);
        let step2 = MockProcessingStep::new("step2", mock_step_modify_content);
        let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(step1), Box::new(step2)];
        let _executor = PipelineExecutor::new(steps);
        // The internal steps vector is private, so we cannot assert its length directly.
        // Successful creation is sufficient for this test.
    }

    #[tokio::test] // Use tokio::test for async tests
    async fn test_run_single_async_empty_pipeline() {
        let steps: Vec<Box<dyn ProcessingStep>> = vec![];
        let executor = PipelineExecutor::new(steps);
        let doc = create_test_document("doc1", "initial content");
        let original_content = doc.content.clone();

        let result_doc = executor.run_single_async(doc).await.unwrap();
        assert_eq!(result_doc.content, original_content);
    }

    #[tokio::test]
    async fn test_run_single_async_one_step() {
        let step1 = MockProcessingStep::new("step1_modify", |mut doc| {
            doc.content = "modified by step1".to_string();
            Ok(doc)
        });
        let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(step1)];
        let executor = PipelineExecutor::new(steps);
        let doc = create_test_document("doc1", "initial content");

        let result_doc = executor.run_single_async(doc).await.unwrap();
        assert_eq!(result_doc.content, "modified by step1");
    }

    #[tokio::test]
    async fn test_run_single_async_multiple_steps() {
        let step1 = MockProcessingStep::new("step1_append", |mut doc| {
            doc.content.push_str(" + step1");
            Ok(doc)
        });
        let step2 = MockProcessingStep::new("step2_append", |mut doc| {
            doc.content.push_str(" + step2");
            Ok(doc)
        });
        let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(step1), Box::new(step2)];
        let executor = PipelineExecutor::new(steps);
        let doc = create_test_document("doc1", "initial");

        let result_doc = executor.run_single_async(doc).await.unwrap();
        assert_eq!(result_doc.content, "initial + step1 + step2");
    }

    #[tokio::test]
    async fn test_run_single_async_step_error() {
        let step1 = MockProcessingStep::new("step1_ok", |mut doc| {
            doc.content.push_str(" + step1");
            Ok(doc)
        });
        let error_step = MockProcessingStep::new_error_step("error_step", "Something went wrong");
        let step3_never_runs = MockProcessingStep::new("step3_never_runs", |mut doc| {
            doc.content.push_str(" + step3");
            Ok(doc)
        });

        let steps: Vec<Box<dyn ProcessingStep>> = vec![
            Box::new(step1),
            Box::new(error_step),
            Box::new(step3_never_runs),
        ];
        let executor = PipelineExecutor::new(steps);
        let doc = create_test_document("doc1", "initial");

        let result = executor.run_single_async(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::StepError {
            step_name,
            source: _,
        }) = result
        {
            assert_eq!(step_name, "error_step");
        } else {
            panic!("Expected a StepError, but got {:?}", result);
        }

        // We can also check the content of the document if it were returned with the error,
        // but based on current run_single_async it only returns Result<TextDocument>.
        // If we want to check partial processing, the error type or return type would need to change.
    }

    #[tokio::test]
    async fn test_run_batch_parallel_async_empty_documents() {
        let steps: Vec<Box<dyn ProcessingStep>> = vec![Box::new(MockProcessingStep::new(
            "step1",
            mock_step_passthrough,
        ))];
        let executor = PipelineExecutor::new(steps);
        let documents: Vec<TextDocument> = vec![];

        let results = executor.run_batch_parallel_async(documents).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_run_batch_parallel_async_empty_pipeline() {
        let steps: Vec<Box<dyn ProcessingStep>> = vec![];
        let executor = PipelineExecutor::new(steps);
        let documents = vec![
            create_test_document("doc1", "content1"),
            create_test_document("doc2", "content2"),
        ];
        let original_contents: Vec<String> = documents.iter().map(|d| d.content.clone()).collect();

        let results = executor.run_batch_parallel_async(documents).await;
        assert_eq!(results.len(), 2);
        for (i, result) in results.into_iter().enumerate() {
            let processed_doc = result.unwrap();
            assert_eq!(processed_doc.content, original_contents[i]);
        }
    }

    #[tokio::test]
    async fn test_run_batch_parallel_async_multiple_documents_and_steps() {
        let step1 = MockProcessingStep::new("step1_append", |mut doc| {
            doc.content.push_str(" + step1");
            Ok(doc)
        });
        let step2 = MockProcessingStep::new("step2_append", |mut doc| {
            doc.content.push_str(" + step2");
            Ok(doc)
        });

        let steps: Vec<Box<dyn ProcessingStep>> = vec![
            Box::new(MockProcessingStep::new("step1_append", |mut doc| {
                doc.content.push_str(" + step1");
                Ok(doc)
            })),
            Box::new(MockProcessingStep::new("step2_append", |mut doc| {
                doc.content.push_str(" + step2");
                Ok(doc)
            })),
        ];
        let executor = PipelineExecutor::new(steps);

        let documents = vec![
            create_test_document("doc1", "doc1_initial"),
            create_test_document("doc2", "doc2_initial"),
        ];

        let results = executor.run_batch_parallel_async(documents).await;
        assert_eq!(results.len(), 2);

        let processed_content1 = results[0].as_ref().unwrap().content.clone();
        assert_eq!(processed_content1, "doc1_initial + step1 + step2");

        let processed_content2 = results[1].as_ref().unwrap().content.clone();
        assert_eq!(processed_content2, "doc2_initial + step1 + step2");
    }

    #[tokio::test]
    async fn test_run_batch_parallel_async_with_errors() {
        let steps: Vec<Box<dyn ProcessingStep>> = vec![
            Box::new(MockProcessingStep::new("step_ok", |mut doc| {
                doc.content.push_str(" + ok");
                Ok(doc)
            })),
            Box::new(MockProcessingStep::new_error_step(
                "step_err",
                "Batch processing error",
            )),
            Box::new(MockProcessingStep::new("step_ok_after_error", |mut doc| {
                doc.content.push_str(" + after_error");
                Ok(doc)
            })),
        ];
        let executor = PipelineExecutor::new(steps);

        let documents = vec![
            create_test_document("doc1_ok_then_err", "doc1"), // This will fail at step_err
            create_test_document("doc2_ok_then_err", "doc2"), // This will also fail at step_err
        ];

        let results = executor.run_batch_parallel_async(documents).await;
        assert_eq!(results.len(), 2);

        // Check first document's result (should be an error)
        assert!(results[0].is_err());
        if let Err(PipelineError::StepError { step_name, .. }) = &results[0] {
            assert_eq!(step_name, "step_err");
        } else {
            panic!("Expected StepError for doc1, got {:?}", results[0]);
        }

        // Check second document's result (should also be an error)
        assert!(results[1].is_err());
        if let Err(PipelineError::StepError { step_name, .. }) = &results[1] {
            assert_eq!(step_name, "step_err");
        } else {
            panic!("Expected StepError for doc2, got {:?}", results[1]);
        }
    }

    #[tokio::test]
    async fn test_run_batch_parallel_async_mixed_success_and_failure() {
        // Mock step that errors only for a specific document ID
        struct SmartErrorStep {
            error_doc_id: String,
        }
        #[async_trait]
        impl ProcessingStep for SmartErrorStep {
            fn name(&self) -> &'static str {
                "smart_error_step"
            }
            async fn process(&self, mut document: TextDocument) -> Result<TextDocument> {
                if document.id == self.error_doc_id {
                    Err(PipelineError::StepError {
                        step_name: self.name().to_string(),
                        source: Box::new(PipelineError::IoError {
                            source: std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "Error for specific doc",
                            ),
                        }),
                    })
                } else {
                    document.content.push_str(" + processed_by_smart_step");
                    Ok(document)
                }
            }
        }

        let steps: Vec<Box<dyn ProcessingStep>> = vec![
            Box::new(MockProcessingStep::new(
                "step1_passthrough",
                mock_step_passthrough,
            )),
            Box::new(SmartErrorStep {
                error_doc_id: "doc_to_fail".to_string(),
            }),
        ];
        let executor = PipelineExecutor::new(steps);

        let documents = vec![
            create_test_document("doc_success", "content_success"),
            create_test_document("doc_to_fail", "content_fail"),
        ];

        let results = executor.run_batch_parallel_async(documents).await;
        assert_eq!(results.len(), 2);

        // First document should succeed
        assert!(results[0].is_ok());
        if let Ok(doc) = &results[0] {
            assert_eq!(doc.id, "doc_success");
            assert!(doc.content.contains("+ processed_by_smart_step"));
        }

        // Second document should fail
        assert!(results[1].is_err());
        if let Err(PipelineError::StepError { step_name, .. }) = &results[1] {
            assert_eq!(*step_name, "smart_error_step");
        } else {
            panic!("Expected StepError for doc_to_fail, got {:?}", results[1]);
        }
    }
}
