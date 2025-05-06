// src/config.rs
use serde::Deserialize;

// Keep existing config
#[derive(Deserialize, Debug, Clone)]
pub struct ParquetInputConfig {
    pub path: String,              // Path to the Parquet file or directory
    pub text_column: String,       // Name of the column containing the main text
    pub id_column: Option<String>, // Optional: Name of a column to use as document ID
    // Add other column mappings as needed (e.g., for metadata)
    pub batch_size: Option<usize>, // Optional: Arrow batch size for reading
}

// {{ Add pipeline configuration structs below }}

// --- Pipeline Configuration ---

/// Represents the overall pipeline configuration read from YAML.
#[derive(Deserialize, Debug, Clone)]
pub struct PipelineConfig {
    pub pipeline: Vec<StepConfig>,
}

/// Represents a single step in the processing pipeline.
/// Uses Serde's externally tagged enum representation.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")] // The 'type' field in YAML determines which variant
pub enum StepConfig {
    C4QualityFilter(C4QualityParams),
    GopherRepetitionFilter(GopherRepetitionParams),
    GopherQualityFilter(GopherQualityParams),
    // Add other filter/step types here as needed
}

/// Parameters for the C4QualityFilter.
#[derive(Deserialize, Debug, Clone)]
pub struct C4QualityParams {
    pub min_sentences: usize,
    pub min_words_per_sentence: usize,
    pub max_word_length: usize,
}

/// Parameters for the GopherRepetitionFilter.
#[derive(Deserialize, Debug, Clone)]
pub struct GopherRepetitionParams {
    // Use Option<f64> for optional fields
    pub dup_line_frac: Option<f64>,
    pub dup_para_frac: Option<f64>,
    pub dup_line_char_frac: Option<f64>,
    pub dup_para_char_frac: Option<f64>,
    // Vec<(usize, f64)> represents lists like [[2, 0.2], [3, 0.18]] in YAML
    #[serde(default)] // Use default (empty vec) if not specified
    pub top_n_grams: Vec<(usize, f64)>,
    #[serde(default)]
    pub dup_n_grams: Vec<(usize, f64)>,
}

/// Parameters for the GopherQualityFilter.
#[derive(Deserialize, Debug, Clone)]
pub struct GopherQualityParams {
    // Use Option for optional fields
    pub min_doc_words: Option<usize>,
    pub max_doc_words: Option<usize>,
    pub min_avg_word_length: Option<f64>,
    pub max_avg_word_length: Option<f64>,
    pub max_symbol_word_ratio: Option<f64>,
    pub max_bullet_lines_ratio: Option<f64>,
    pub max_ellipsis_lines_ratio: Option<f64>,
    pub max_non_alpha_words_ratio: Option<f64>,
    pub min_stop_words: Option<usize>,
    // Optional list of stop words; if None, the filter's default will be used.
    pub stop_words: Option<Vec<String>>,
}
