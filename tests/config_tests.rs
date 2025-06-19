// {{ Add unit tests for load_pipeline_config }}
#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use TextBlaster::config::pipeline::{load_pipeline_config, StepConfig};
    use TextBlaster::error::PipelineError;

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
