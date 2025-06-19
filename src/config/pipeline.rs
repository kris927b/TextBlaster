use crate::error::{PipelineError, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs; // For reading the file
use std::path::Path; // For path handling // Assuming these are your error types

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
