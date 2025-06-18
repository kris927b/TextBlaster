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

impl PipelineConfig {
    pub fn validate(&self) -> Result<()> {
        for step_config in &self.pipeline {
            step_config.validate()?;
        }
        Ok(())
    }
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

    pub fn validate(&self) -> Result<()> {
        match self {
            StepConfig::C4QualityFilter(params) => params.validate(),
            StepConfig::GopherRepetitionFilter(params) => params.validate(),
            StepConfig::GopherQualityFilter(params) => params.validate(),
            StepConfig::C4BadWordsFilter(params) => params.validate(),
            StepConfig::LanguageDetectionFilter(params) => params.validate(),
            StepConfig::FineWebQualityFilter(params) => params.validate(),
            StepConfig::TokenCounter(params) => params.validate(),
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

impl C4QualityParams {
    pub fn validate(&self) -> Result<()> {
        if self.min_num_sentences == 0 {
            return Err(PipelineError::ConfigValidationError(
                "C4QualityParams: min_num_sentences must be greater than 0".to_string(),
            ));
        }
        if self.min_words_per_line == 0 {
            return Err(PipelineError::ConfigValidationError(
                "C4QualityParams: min_words_per_line must be greater than 0".to_string(),
            ));
        }
        if self.max_word_length == 0 {
            return Err(PipelineError::ConfigValidationError(
                "C4QualityParams: max_word_length must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
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

impl GopherRepetitionParams {
    pub fn validate(&self) -> Result<()> {
        let fractions = [
            ("dup_line_frac", self.dup_line_frac),
            ("dup_para_frac", self.dup_para_frac),
            ("dup_line_char_frac", self.dup_line_char_frac),
            ("dup_para_char_frac", self.dup_para_char_frac),
        ];
        for (name, val) in fractions.iter() {
            if let Some(v) = val {
                if !(0.0..=1.0).contains(v) {
                    return Err(PipelineError::ConfigValidationError(format!(
                        "GopherRepetitionParams: {} must be between 0.0 and 1.0, got {}",
                        name, v
                    )));
                }
            }
        }

        for (name, n_grams) in [
            ("top_n_grams", &self.top_n_grams),
            ("dup_n_grams", &self.dup_n_grams),
        ]
        .iter()
        {
            for (idx, (size, fraction)) in n_grams.iter().enumerate() {
                if *size == 0 {
                    return Err(PipelineError::ConfigValidationError(format!(
                        "GopherRepetitionParams: n-gram size in {} at index {} must be greater than 0",
                        name, idx
                    )));
                }
                if !(0.0..=1.0).contains(fraction) {
                    return Err(PipelineError::ConfigValidationError(format!(
                        "GopherRepetitionParams: n-gram fraction in {} at index {} must be between 0.0 and 1.0, got {}",
                        name, idx, fraction
                    )));
                }
            }
        }
        Ok(())
    }
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

impl GopherQualityParams {
    pub fn validate(&self) -> Result<()> {
        if let Some(min_doc_words) = self.min_doc_words {
            if min_doc_words == 0 {
                return Err(PipelineError::ConfigValidationError(
                    "GopherQualityParams: min_doc_words must be greater than 0".to_string(),
                ));
            }
        }
        if let Some(max_doc_words) = self.max_doc_words {
            if max_doc_words == 0 {
                return Err(PipelineError::ConfigValidationError(
                    "GopherQualityParams: max_doc_words must be greater than 0".to_string(),
                ));
            }
        }
        if let (Some(min_val), Some(max_val)) = (self.min_doc_words, self.max_doc_words) {
            if min_val > max_val {
                return Err(PipelineError::ConfigValidationError(format!(
                    "GopherQualityParams: min_doc_words ({}) cannot be greater than max_doc_words ({})",
                    min_val, max_val
                )));
            }
        }

        if let Some(min_avg_word_length) = self.min_avg_word_length {
            if min_avg_word_length <= 0.0 {
                return Err(PipelineError::ConfigValidationError(
                    "GopherQualityParams: min_avg_word_length must be greater than 0.0".to_string(),
                ));
            }
        }
        if let Some(max_avg_word_length) = self.max_avg_word_length {
            if max_avg_word_length <= 0.0 {
                return Err(PipelineError::ConfigValidationError(
                    "GopherQualityParams: max_avg_word_length must be greater than 0.0".to_string(),
                ));
            }
        }
        if let (Some(min_val), Some(max_val)) = (self.min_avg_word_length, self.max_avg_word_length)
        {
            if min_val > max_val {
                return Err(PipelineError::ConfigValidationError(format!(
                    "GopherQualityParams: min_avg_word_length ({}) cannot be greater than max_avg_word_length ({})",
                    min_val, max_val
                )));
            }
        }

        let ratio_params = [
            ("max_symbol_word_ratio", self.max_symbol_word_ratio),
            ("max_bullet_lines_ratio", self.max_bullet_lines_ratio),
            ("max_ellipsis_lines_ratio", self.max_ellipsis_lines_ratio),
            ("max_non_alpha_words_ratio", self.max_non_alpha_words_ratio),
        ];
        for (name, val) in ratio_params.iter() {
            if let Some(v) = val {
                if *v < 0.0 {
                    return Err(PipelineError::ConfigValidationError(format!(
                        "GopherQualityParams: {} must be non-negative, got {}",
                        name, v
                    )));
                }
            }
        }

        // This test is pointless...
        // if let Some(min_stop_words) = self.min_stop_words {
        //     // min_stop_words can be 0, so no check for > 0 needed here.
        //     // This is valid as per the original description "greater than or equal to 0".
        //     if min_stop_words < 0 {
        //         return Err(PipelineError::ConfigValidationError(format!(
        //             "GopherQualityParams: min_stop_words must be non-negative, got {}",
        //             min_stop_words
        //         )));
        //     }
        // }

        Ok(())
    }
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

impl C4BadWordsParams {
    pub fn validate(&self) -> Result<()> {
        if !(0.0..=1.0).contains(&self.keep_fraction) {
            return Err(PipelineError::ConfigValidationError(format!(
                "C4BadWordsParams: keep_fraction must be between 0.0 and 1.0, got {}",
                self.keep_fraction
            )));
        }
        if self.default_language.is_empty() {
            return Err(PipelineError::ConfigValidationError(
                "C4BadWordsParams: default_language cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

// Parameters for the LangaugeDetectionFilter
#[derive(Deserialize, Debug, Clone)]
pub struct LanguageDetectionParams {
    pub min_confidence: f64,
    pub allowed_languages: Vec<String>,
}

impl LanguageDetectionParams {
    pub fn validate(&self) -> Result<()> {
        if !(0.0..=1.0).contains(&self.min_confidence) {
            return Err(PipelineError::ConfigValidationError(format!(
                "LanguageDetectionParams: min_confidence must be between 0.0 and 1.0, got {}",
                self.min_confidence
            )));
        }
        if self.allowed_languages.is_empty() {
            return Err(PipelineError::ConfigValidationError(
                "LanguageDetectionParams: allowed_languages cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
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

impl FineWebQualityFilterParams {
    pub fn validate(&self) -> Result<()> {
        let params_to_check = [
            ("line_punct_thr", self.line_punct_thr),
            ("short_line_thr", self.short_line_thr),
            ("char_duplicates_ratio", self.char_duplicates_ratio),
            ("new_line_ratio", self.new_line_ratio),
        ];

        for (name, value) in params_to_check.iter() {
            if !(0.0..=1.0).contains(value) {
                return Err(PipelineError::ConfigValidationError(format!(
                    "FineWebQualityFilterParams: {} must be between 0.0 and 1.0, got {}",
                    name, value
                )));
            }
        }

        if self.short_line_length == 0 {
            return Err(PipelineError::ConfigValidationError(
                "FineWebQualityFilterParams: short_line_length must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

// Parameters for the TokenCounter
#[derive(Deserialize, Debug, Clone)]
pub struct TokenCounterParams {
    pub tokenizer_name: String,
}

impl TokenCounterParams {
    pub fn validate(&self) -> Result<()> {
        // Add specific validation logic for TokenCounterParams if needed in the future
        // For example, check if tokenizer_name is not empty or refers to a known tokenizer
        if self.tokenizer_name.is_empty() {
            return Err(PipelineError::ConfigValidationError(
                "TokenCounterParams: tokenizer_name cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
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

    let config: PipelineConfig = serde_yaml::from_str(&config_content).map_err(|e| {
        PipelineError::ConfigError(format!(
            "Failed to parse pipeline config YAML from '{}': {}",
            path_ref.display(),
            e
        ))
    })?;

    config.validate()?; // Validate the loaded configuration

    Ok(config)
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

    // Helper macro for asserting ConfigValidationError
    macro_rules! assert_config_validation_error {
        ($result:expr, $expected_msg_part:expr) => {
            match $result {
                Err(PipelineError::ConfigValidationError(msg)) => {
                    assert!(
                        msg.contains($expected_msg_part),
                        "Error message '{}' did not contain '{}'",
                        msg,
                        $expected_msg_part
                    );
                }
                Err(other_err) => {
                    panic!(
                        "Expected ConfigValidationError, but got different error: {:?}",
                        other_err
                    );
                }
                Ok(_) => {
                    panic!("Expected error, but got Ok");
                }
            }
        };
        ($result:expr) => {
            match $result {
                Err(PipelineError::ConfigValidationError(_)) => {
                    // Expected error type, no message check
                }
                Err(other_err) => {
                    panic!(
                        "Expected ConfigValidationError, but got different error: {:?}",
                        other_err
                    );
                }
                Ok(_) => {
                    panic!("Expected error, but got Ok");
                }
            }
        };
    }

    // --- C4QualityParams Tests ---
    fn default_c4_quality_params() -> C4QualityParams {
        C4QualityParams {
            split_paragraph: false,
            remove_citations: true,
            filter_no_terminal_punct: true,
            min_num_sentences: 1,
            min_words_per_line: 1,
            max_word_length: 1,
            filter_lorem_ipsum: true,
            filter_javascript: true,
            filter_curly_bracket: true,
            filter_policy: true,
        }
    }

    #[test]
    fn test_c4_quality_params_valid() {
        let params = default_c4_quality_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_c4_quality_params_invalid_min_num_sentences() {
        let params = C4QualityParams {
            min_num_sentences: 0,
            ..default_c4_quality_params()
        };
        assert_config_validation_error!(params.validate(), "min_num_sentences");
    }

    #[test]
    fn test_c4_quality_params_invalid_min_words_per_line() {
        let params = C4QualityParams {
            min_words_per_line: 0,
            ..default_c4_quality_params()
        };
        assert_config_validation_error!(params.validate(), "min_words_per_line");
    }

    #[test]
    fn test_c4_quality_params_invalid_max_word_length() {
        let params = C4QualityParams {
            max_word_length: 0,
            ..default_c4_quality_params()
        };
        assert_config_validation_error!(params.validate(), "max_word_length");
    }

    // --- GopherRepetitionParams Tests ---
    fn default_gopher_repetition_params() -> GopherRepetitionParams {
        GopherRepetitionParams {
            dup_line_frac: Some(0.5),
            dup_para_frac: Some(0.5),
            dup_line_char_frac: Some(0.5),
            dup_para_char_frac: Some(0.5),
            top_n_grams: vec![(2, 0.5), (3, 0.5)],
            dup_n_grams: vec![(2, 0.5), (3, 0.5)],
        }
    }

    #[test]
    fn test_gopher_repetition_params_valid() {
        let params = default_gopher_repetition_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_gopher_repetition_params_invalid_frac() {
        let mut params = default_gopher_repetition_params();
        params.dup_line_frac = Some(1.1);
        assert_config_validation_error!(params.validate(), "dup_line_frac");

        params = default_gopher_repetition_params();
        params.dup_para_frac = Some(-0.1);
        assert_config_validation_error!(params.validate(), "dup_para_frac");
    }

    #[test]
    fn test_gopher_repetition_params_invalid_ngram_size() {
        let mut params = default_gopher_repetition_params();
        params.top_n_grams = vec![(0, 0.5)];
        assert_config_validation_error!(params.validate(), "n-gram size");
    }

    #[test]
    fn test_gopher_repetition_params_invalid_ngram_fraction() {
        let mut params = default_gopher_repetition_params();
        params.dup_n_grams = vec![(2, 1.1)];
        assert_config_validation_error!(params.validate(), "n-gram fraction");
    }

    // --- GopherQualityParams Tests ---
    fn default_gopher_quality_params() -> GopherQualityParams {
        GopherQualityParams {
            min_doc_words: Some(10),
            max_doc_words: Some(1000),
            min_avg_word_length: Some(3.0),
            max_avg_word_length: Some(10.0),
            max_symbol_word_ratio: Some(0.1),
            max_bullet_lines_ratio: Some(0.1),
            max_ellipsis_lines_ratio: Some(0.1),
            max_non_alpha_words_ratio: Some(0.1),
            min_stop_words: Some(0),
            stop_words: None,
        }
    }

    #[test]
    fn test_gopher_quality_params_valid() {
        let params = default_gopher_quality_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_gopher_quality_params_invalid_min_doc_words_zero() {
        let params = GopherQualityParams {
            min_doc_words: Some(0),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(params.validate(), "min_doc_words");
    }

    #[test]
    fn test_gopher_quality_params_invalid_max_doc_words_zero() {
        let params = GopherQualityParams {
            max_doc_words: Some(0),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(params.validate(), "max_doc_words");
    }

    #[test]
    fn test_gopher_quality_params_invalid_min_greater_than_max_doc_words() {
        let params = GopherQualityParams {
            min_doc_words: Some(100),
            max_doc_words: Some(10),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(
            params.validate(),
            "min_doc_words (100) cannot be greater than max_doc_words (10)"
        );
    }

    #[test]
    fn test_gopher_quality_params_invalid_min_avg_word_length_zero() {
        let params = GopherQualityParams {
            min_avg_word_length: Some(0.0),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(params.validate(), "min_avg_word_length");
    }

    #[test]
    fn test_gopher_quality_params_invalid_max_avg_word_length_zero() {
        let params = GopherQualityParams {
            max_avg_word_length: Some(0.0),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(params.validate(), "max_avg_word_length");
    }

    #[test]
    fn test_gopher_quality_params_invalid_min_greater_than_max_avg_word_length() {
        let params = GopherQualityParams {
            min_avg_word_length: Some(10.0),
            max_avg_word_length: Some(3.0),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(
            params.validate(),
            "min_avg_word_length (10) cannot be greater than max_avg_word_length (3)"
        );
    }

    #[test]
    fn test_gopher_quality_params_invalid_ratio_negative() {
        let params = GopherQualityParams {
            max_symbol_word_ratio: Some(-0.1),
            ..default_gopher_quality_params()
        };
        assert_config_validation_error!(
            params.validate(),
            "max_symbol_word_ratio must be non-negative"
        );
    }

    // --- C4BadWordsParams Tests ---
    fn default_c4_bad_words_params() -> C4BadWordsParams {
        C4BadWordsParams {
            keep_fraction: 0.5,
            fail_on_missing_language: false,
            seed: None,
            default_language: "en".to_string(),
            cache_base_path: None,
        }
    }

    #[test]
    fn test_c4_bad_words_params_valid() {
        let params = default_c4_bad_words_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_c4_bad_words_params_invalid_keep_fraction_too_high() {
        let params = C4BadWordsParams {
            keep_fraction: 1.1,
            ..default_c4_bad_words_params()
        };
        assert_config_validation_error!(params.validate(), "keep_fraction");
    }

    #[test]
    fn test_c4_bad_words_params_invalid_keep_fraction_negative() {
        let params = C4BadWordsParams {
            keep_fraction: -0.1,
            ..default_c4_bad_words_params()
        };
        assert_config_validation_error!(params.validate(), "keep_fraction");
    }

    #[test]
    fn test_c4_bad_words_params_invalid_default_language_empty() {
        let params = C4BadWordsParams {
            default_language: "".to_string(),
            ..default_c4_bad_words_params()
        };
        assert_config_validation_error!(params.validate(), "default_language");
    }

    // --- LanguageDetectionParams Tests ---
    fn default_language_detection_params() -> LanguageDetectionParams {
        LanguageDetectionParams {
            min_confidence: 0.5,
            allowed_languages: vec!["en".to_string(), "fr".to_string()],
        }
    }

    #[test]
    fn test_language_detection_params_valid() {
        let params = default_language_detection_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_language_detection_params_invalid_min_confidence_too_high() {
        let params = LanguageDetectionParams {
            min_confidence: 1.1,
            ..default_language_detection_params()
        };
        assert_config_validation_error!(params.validate(), "min_confidence");
    }

    #[test]
    fn test_language_detection_params_invalid_min_confidence_negative() {
        let params = LanguageDetectionParams {
            min_confidence: -0.1,
            ..default_language_detection_params()
        };
        assert_config_validation_error!(params.validate(), "min_confidence");
    }

    #[test]
    fn test_language_detection_params_invalid_allowed_languages_empty() {
        let params = LanguageDetectionParams {
            allowed_languages: vec![],
            ..default_language_detection_params()
        };
        assert_config_validation_error!(params.validate(), "allowed_languages");
    }

    // --- FineWebQualityFilterParams Tests ---
    fn default_fine_web_quality_filter_params() -> FineWebQualityFilterParams {
        FineWebQualityFilterParams {
            line_punct_thr: 0.5,
            line_punct_exclude_zero: false,
            stop_chars: None,
            short_line_thr: 0.5,
            short_line_length: 10,
            char_duplicates_ratio: 0.5,
            new_line_ratio: 0.5,
        }
    }

    #[test]
    fn test_fine_web_quality_filter_params_valid() {
        let params = default_fine_web_quality_filter_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_fine_web_quality_filter_params_invalid_line_punct_thr_too_high() {
        let params = FineWebQualityFilterParams {
            line_punct_thr: 1.1,
            ..default_fine_web_quality_filter_params()
        };
        assert_config_validation_error!(params.validate(), "line_punct_thr");
    }

    #[test]
    fn test_fine_web_quality_filter_params_invalid_line_punct_thr_negative() {
        let params = FineWebQualityFilterParams {
            line_punct_thr: -0.1,
            ..default_fine_web_quality_filter_params()
        };
        assert_config_validation_error!(params.validate(), "line_punct_thr");
    }

    #[test]
    fn test_fine_web_quality_filter_params_invalid_short_line_length_zero() {
        let params = FineWebQualityFilterParams {
            short_line_length: 0,
            ..default_fine_web_quality_filter_params()
        };
        assert_config_validation_error!(params.validate(), "short_line_length");
    }

    // --- TokenCounterParams Tests ---
    fn default_token_counter_params() -> TokenCounterParams {
        TokenCounterParams {
            tokenizer_name: "gpt2".to_string(),
        }
    }

    #[test]
    fn test_token_counter_params_valid() {
        let params = default_token_counter_params();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_token_counter_params_invalid_tokenizer_name_empty() {
        let params = TokenCounterParams {
            tokenizer_name: "".to_string(),
        };
        assert_config_validation_error!(params.validate(), "tokenizer_name");
    }

    // --- load_pipeline_config validation tests ---
    #[test]
    fn test_load_pipeline_config_invalid_step_validation() {
        let yaml_content = r#"
pipeline:
  - type: C4QualityFilter
    split_paragraph: false
    remove_citations: true
    filter_no_terminal_punct: true
    min_num_sentences: 0 # Invalid value
    min_words_per_line: 3
    max_word_length: 15
    filter_lorem_ipsum: true
    filter_javascript: true
    filter_curly_bracket: true
    filter_policy: true
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let result = load_pipeline_config(temp_file.path());
        assert_config_validation_error!(result, "min_num_sentences");
    }

    #[test]
    fn test_load_pipeline_config_invalid_language_detection_validation() {
        let yaml_content = r#"
pipeline:
  - type: LanguageDetectionFilter
    min_confidence: 1.5 # Invalid value
    allowed_languages: ["en", "fr"]
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let result = load_pipeline_config(temp_file.path());
        assert_config_validation_error!(result, "min_confidence");
    }

    #[test]
    fn test_load_pipeline_config_invalid_token_counter_validation() {
        let yaml_content = r#"
pipeline:
  - type: TokenCounter
    tokenizer_name: "" # Invalid value
        "#;
        let temp_file = create_temp_config_file(yaml_content);
        let result = load_pipeline_config(temp_file.path());
        assert_config_validation_error!(result, "tokenizer_name");
    }
}
