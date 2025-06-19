use crate::error::{PipelineError, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs; // For reading the file
use std::path::Path; // For path handling // Assuming these are your error types

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
    C4BadWordsFilter(C4BadWordsParams), // New
    LanguageDetectionFilter(LanguageDetectionParams),
    FineWebQualityFilter(FineWebQualityFilterParams), // Renamed and new params struct
    TokenCounter(TokenCounterParams),
    // Add other filter/step types here as needed
}

impl StepConfig {
    /// Returns a string slice representing the name of the step type.
    pub fn name(&self) -> &'static str {
        match self {
            StepConfig::C4QualityFilter(_) => "C4QualityFilter",
            StepConfig::GopherRepetitionFilter(_) => "GopherRepetitionFilter",
            StepConfig::GopherQualityFilter(_) => "GopherQualityFilter",
            StepConfig::C4BadWordsFilter(_) => "C4BadWordsFilter", // New
            StepConfig::LanguageDetectionFilter(_) => "LanguageDetectionFilter",
            StepConfig::FineWebQualityFilter(_) => "FineWebQualityFilter", // Renamed
            StepConfig::TokenCounter(_) => "TokenCounter", // Add cases for other StepConfig variants here
        }
    }
}

/// Parameters for the C4QualityFilter.
#[derive(Deserialize, Debug, Clone)]
pub struct C4QualityParams {
    pub split_paragraph: bool,
    pub remove_citations: bool,
    pub filter_no_terminal_punct: bool,
    pub min_num_sentences: usize,
    pub min_words_per_line: usize,
    pub max_word_length: usize,
    pub filter_lorem_ipsum: bool,
    pub filter_javascript: bool,
    pub filter_curly_bracket: bool,
    pub filter_policy: bool,
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

#[derive(Deserialize, Debug, Clone)]
pub struct C4BadWordsParams {
    pub keep_fraction: f32,
    pub fail_on_missing_language: bool,
    pub seed: Option<u64>,
    pub default_language: String,
    #[serde(skip)] // This field will not be deserialized from YAML
    pub cache_base_path: Option<std::path::PathBuf>,
}

// Parameters for the LangaugeDetectionFilter
#[derive(Deserialize, Debug, Clone)]
pub struct LanguageDetectionParams {
    pub min_confidence: f64,
    pub allowed_languages: Vec<String>,
}

// Parameters for the FineWebQualityFilter (new filter based on Python logic).
#[derive(Deserialize, Debug, Clone, Default)] // Added Default for easier construction in worker
pub struct FineWebQualityFilterParams {
    pub line_punct_thr: f64,
    pub line_punct_exclude_zero: bool,
    pub stop_chars: Option<HashSet<char>>, // Will be converted to HashSet<char> in setup
    pub short_line_thr: f64,
    pub short_line_length: usize, // serde will handle u64 -> usize if value fits
    pub char_duplicates_ratio: f64,
    pub new_line_ratio: f64,
    // pub language: String,
}

// Parameters for the TokenCounter
#[derive(Deserialize, Debug, Clone)]
pub struct TokenCounterParams {
    pub tokenizer_name: String,
}

// {{ Add the new function to load pipeline configuration }}
/// Loads and parses the pipeline configuration YAML file.
pub fn load_pipeline_config<P: AsRef<Path>>(config_path: P) -> Result<PipelineConfig> {
    let path_ref = config_path.as_ref();
    let config_content = fs::read_to_string(path_ref).map_err(|e| {
        PipelineError::ConfigError(format!(
            "Failed to read pipeline config file '{}': {}",
            path_ref.display(),
            e
        ))
    })?;

    serde_yaml::from_str(&config_content).map_err(|e| {
        PipelineError::ConfigError(format!(
            "Failed to parse pipeline config YAML from '{}': {}",
            path_ref.display(),
            e
        ))
    })
}
