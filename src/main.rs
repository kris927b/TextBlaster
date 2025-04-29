mod config;
mod data_model;
mod error;
mod executor;
mod pipeline;
mod utils;

use crate::config::ParquetInputConfig;
use crate::data_model::TextDocument;
use crate::executor::PipelineExecutor;
use crate::executor::ProcessingStep;
use crate::pipeline::filters::{C4QualityFilter, GopherQualityFilter, GopherRepetitionFilter};
use crate::pipeline::readers::ParquetReader;
use crate::utils::text::DANISH_STOP_WORDS;
use arrow::datatypes::ArrowNativeType;
// Assuming you load this
use error::Result; // Your project's result type

fn main() -> Result<()> {
    // 1. Load Configuration (including ParquetInputConfig)
    // let app_config = config::load_app_config()?; // Load your overall config
    // let parquet_config = app_config.input.parquet; // Example structure
    // --- Placeholder config for demonstration ---
    let parquet_config = ParquetInputConfig {
        path: "data/danske-taler.parquet".to_string(), // <--- CHANGE THIS PATH
        text_column: "text".to_string(),               // <--- CHANGE COLUMN NAME
        id_column: Some("id".to_string()),             // <--- CHANGE or set to None
        batch_size: Some(32),
    };
    // --- End Placeholder ---

    // 2. Create the Parquet Reader
    let reader = ParquetReader::new(parquet_config);

    // 3. Read documents into a Vec (or process the iterator directly)
    //    Handle potential errors during reading/collection
    println!("Reading documents...");
    let documents: Vec<TextDocument> = reader
        .read_documents()? // Get the iterator
        .filter_map(|result| {
            // Process results, filter out errors for now (or handle differently)
            match result {
                Ok(doc) => Some(doc),
                Err(e) => {
                    eprintln!("Error reading document: {}", e); // Log errors
                    None // Skip documents that failed to read
                }
            }
        })
        .collect(); // Collect into memory

    println!("Read {} documents.", documents.len());

    if documents.is_empty() {
        println!("No documents read, exiting.");
        return Ok(());
    }

    // 4. Build the processing pipeline (as discussed before)
    // let steps = config::build_pipeline(&app_config)?; // Build steps from config
    // --- Placeholder steps ---
    let steps: Vec<Box<dyn ProcessingStep>> = vec![
        // Box::new(steps::normalize_step::NormalizeStep::new(true)),
        // Box::new(steps::filter_step::FilterStep::new( /* config */ )),
        // ... add actual steps here
        Box::new(C4QualityFilter::new(
            10.as_usize(),
            3.as_usize(),
            100.as_usize(),
        )),
        Box::new(GopherRepetitionFilter::new(
            Some(0.3),
            Some(0.3),
            Some(0.2),
            Some(0.2),
            vec![(2, 0.2), (3, 0.18), (4, 0.16)],
            vec![
                (5, 0.15),
                (6, 0.14),
                (7, 0.13),
                (8, 0.12),
                (9, 0.11),
                (10, 0.10),
            ],
        )),
        Box::new(GopherQualityFilter::new(
            Some(50),
            Some(1000000),
            Some(3.0),
            Some(10.0),
            Some(0.1),
            Some(0.9),
            Some(0.3),
            Some(0.8),
            Some(2),
            // Convert the stop words slice to a Vec<String>
            Some(DANISH_STOP_WORDS.iter().map(|s| s.to_string()).collect()),
        )),
    ];
    // --- End Placeholder ---

    // 5. Create the Executor
    let executor = PipelineExecutor::new(steps);

    // 6. Run the pipeline in parallel
    println!("Processing documents...");
    let results = executor.run_batch_parallel_async(documents); // Process the loaded documents

    // 7. Handle results
    let mut success_count = 0;
    let mut error_count = 0;
    for result in results {
        match result {
            Ok(_processed_doc) => {
                // If the last step wasn't SaveStep, you might save here,
                // or just count successes.
                success_count += 1;
            }
            Err(_e) => {
                // eprintln!("Pipeline error: {}", e);
                error_count += 1;
            }
        }
    }

    println!(
        "Processing complete. Success: {}, Errors: {}",
        success_count, error_count
    );

    Ok(())
}
