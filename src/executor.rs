use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use async_trait::async_trait; // Use async_trait if steps involve async operations
use futures::stream::{FuturesUnordered, StreamExt}; // Ensure futures import is present
use tracing::{debug, warn}; // Added tracing imports

// Use async_trait for async steps
#[async_trait] // {{Add (?Send) marker for object safety}}
pub trait ProcessingStep: Send + Sync {
    // Send + Sync needed for multithreading and async tasks sharing steps
    fn name(&self) -> &'static str; // For logging/error reporting

    // Asynchronous version:
    async fn process(&self, document: TextDocument) -> Result<TextDocument>;
}

pub struct PipelineExecutor {
    steps: Vec<Box<dyn ProcessingStep>>, // Holds the ordered steps
}

impl PipelineExecutor {
    pub fn new(steps: Vec<Box<dyn ProcessingStep>>) -> Self {
        if steps.is_empty() {
            warn!("Pipeline created with no steps.");
        }
        PipelineExecutor { steps }
    }

    // --- Asynchronous Processing ---
    pub async fn run_single_async(&self, initial_document: TextDocument) -> Result<TextDocument> {
        let mut current_doc = initial_document;
        for step in &self.steps {
            debug!("Running async step: {}", step.name());

            // Use map_err to convert the error from the step into a PipelineError::StepError
            // and then use '?' to propagate the error or get the Ok value.
            current_doc = step
                .process(current_doc)
                .await // Await the future from the process step
                .map_err(|e| PipelineError::StepError {
                    // If it's an Err(e)...
                    step_name: step.name().to_string(),
                    // Assuming the error 'e' from process() implements std::error::Error + Send + Sync + 'static
                    // If not, this boxing or the error handling might need adjustment based on the actual type of 'e'.
                    source: Box::new(e),
                })?; // Propagate the error or unwrap the Ok(TextDocument)

            // Replaces the previous match block:
            // match step.process(current_doc).await {
            //      Ok(next_doc) => current_doc = next_doc,
            //      Err(e) => {
            //          return Err(PipelineError::StepError { ... });
            //      }
            //  }
        }
        Ok(current_doc) // Return the final document if all steps succeeded
    }

    // Runs multiple documents concurrently using async tasks
    pub async fn run_batch_parallel_async(
        &self,
        documents: Vec<TextDocument>,
    ) -> Vec<Result<TextDocument>> {
        documents
            .into_iter()
            .map(|doc| self.run_single_async(doc))
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await
    }

    // --- Synchronous methods commented out ---
    /*
    // Process a single document synchronously
    pub fn run_single(&self, initial_document: TextDocument) -> Result<TextDocument> {
        let mut current_doc = initial_document;
        for step in &self.steps {
            println!("Running step: {}", step.name()); // Add proper logging
            match step.process(current_doc) { // Assuming synchronous process existed
                Ok(next_doc) => current_doc = next_doc,
                Err(e) => {
                    // Enrich the error with step information if not already done
                    return Err(PipelineError::StepError {
                        step_name: step.name().to_string(),
                        source: Box::new(e), // Assuming error needs boxing
                    });
                }
            }
        }
        Ok(current_doc)
    }

    // --- Parallel Processing (Example using Rayon) ---
    pub fn run_batch_parallel(&self, documents: Vec<TextDocument>) -> Vec<Result<TextDocument>> {
        use rayon::prelude::*;

        documents
            .into_par_iter() // Rayon's parallel iterator
            .map(|doc| self.run_single(doc)) // Apply the single-doc pipeline to each
            .collect() // Collect the results
    }
    */
}
