// src/config.rs
use crate::error::{PipelineError, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs; // For reading the file
use std::path::Path; // For path handling // Assuming these are your error types

// Keep existing config
#[derive(Deserialize, Debug, Clone)]
pub struct ParquetInputConfig {
    pub path: String,              // Path to the Parquet file or directory
    pub text_column: String,       // Name of the column containing the main text
    pub id_column: Option<String>, // Optional: Name of a column to use as document ID
    // Add other column mappings as needed (e.g., for metadata)
    pub batch_size: Option<usize>, // Optional: Arrow batch size for reading
}

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
    C4BadWordsFilter(C4BadWordsParams), // New
    LanguageDetectionFilter(LanguageDetectionParams),
    FineWebQualityFilter(FineWebQualityFilterParams), // Renamed and new params struct
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
                                                                           // Add cases for other StepConfig variants here
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
    pub language: String,
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

// {{ Add unit tests for load_pipeline_config }}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Helper to create a temporary config file with given content
    fn create_temp_config_file(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        writeln!(temp_file, "{}", content).expect("Failed to write to temp file");
        temp_file
    }

    #[test]
    fn test_load_valid_config() {
        let yaml_content = r#"
pipeline:
  - type: C4QualityFilter
    split_paragraph: false
    remove_citations: true
    filter_no_terminal_punct: true
    min_num_sentences: 5
    min_words_per_line: 3
    max_word_length: 15
    filter_lorem_ipsum: true
    filter_javascript: true
    filter_curly_bracket: true
    filter_policy: true
  - type: GopherRepetitionFilter
    dup_line_frac: 0.20
    top_n_grams: [[2, 0.2], [3, 0.18]]
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let config_result = load_pipeline_config(temp_file.path());

        assert!(
            config_result.is_ok(),
            "Should load valid config: {:?}",
            config_result.err()
        );
        let config = config_result.unwrap();
        assert_eq!(config.pipeline.len(), 2);
        match &config.pipeline[0] {
            StepConfig::C4QualityFilter(params) => {
                assert_eq!(params.min_num_sentences, 5);
            }
            _ => panic!("Expected C4QualityFilter"),
        }
        match &config.pipeline[1] {
            StepConfig::GopherRepetitionFilter(params) => {
                assert_eq!(params.dup_line_frac, Some(0.20));
                assert_eq!(params.top_n_grams.len(), 2);
            }
            _ => panic!("Expected GopherRepetitionFilter"),
        }
    }

    #[test]
    fn test_load_config_file_not_found() {
        let result = load_pipeline_config("non_existent_config.yaml");
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::ConfigError(msg) => {
                assert!(msg.contains("Failed to read pipeline config file"));
                assert!(msg.contains("non_existent_config.yaml"));
            }
            _ => panic!("Expected ConfigError for non-existent file"),
        }
    }

    #[test]
    fn test_load_invalid_yaml_syntax() {
        let yaml_content = r#"
pipeline:
  - type: C4QualityFilter
    min_sentences: 5
  - type: GopherRepetitionFilter
    dup_line_frac: 0.20 # Missing colon after this line would make it invalid
    top_n_grams [[2, 0.2], [3, 0.18]] 
        "#; // Invalid: top_n_grams has no colon and value is not properly indented/formatted
        let temp_file = create_temp_config_file(yaml_content);
        let result = load_pipeline_config(temp_file.path());

        assert!(result.is_err(), "Should fail for invalid YAML syntax");
        match result.err().unwrap() {
            PipelineError::ConfigError(msg) => {
                assert!(msg.contains("Failed to parse pipeline config YAML"));
            }
            _ => panic!("Expected ConfigError for invalid YAML syntax"),
        }
    }

    #[test]
    fn test_load_yaml_unknown_step_type() {
        let yaml_content = r#"
pipeline:
  - type: UnknownFilterType 
    some_param: 123
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let result = load_pipeline_config(temp_file.path());

        assert!(result.is_err(), "Should fail for unknown step type");
        match result.err().unwrap() {
            PipelineError::ConfigError(msg) => {
                assert!(msg.contains("Failed to parse pipeline config YAML"));
                // Serde_yaml error for unknown enum variant might include something like:
                // "unknown variant `UnknownFilterType`, expected one of `C4QualityFilter`, `GopherRepetitionFilter`, `GopherQualityFilter`"
                assert!(msg.contains("UnknownFilterType") || msg.contains("unknown variant"));
            }
            _ => panic!("Expected ConfigError for unknown step type"),
        }
    }

    #[test]
    fn test_load_yaml_missing_pipeline_field() {
        let yaml_content = r#"
# 'pipeline:' field is missing
steps: 
  - type: C4QualityFilter
    min_sentences: 1
    min_words_per_sentence: 1
    max_word_length: 1
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let result = load_pipeline_config(temp_file.path());
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::ConfigError(msg) => {
                assert!(msg.contains("Failed to parse pipeline config YAML"));
                // Serde usually reports missing fields clearly
                assert!(msg.contains("missing field `pipeline`"));
            }
            _ => panic!("Expected ConfigError for missing 'pipeline' field"),
        }
    }

    #[test]
    fn test_load_empty_pipeline_is_valid() {
        let yaml_content = r#"
pipeline: []
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let config_result = load_pipeline_config(temp_file.path());
        assert!(
            config_result.is_ok(),
            "Should load valid config with empty pipeline"
        );
        let config = config_result.unwrap();
        assert!(config.pipeline.is_empty());
    }
}
