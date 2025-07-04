use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::executor::ProcessingStep;
use crate::utils::text::{split_into_words, PUNCTUATION};

use async_trait::async_trait;
use std::collections::HashSet;

/// Default stop words for quality filtering
const DEFAULT_STOP_WORDS: &[&str] = &["the", "be", "to", "of", "and", "that", "have", "with"];

/// A filter applying Gopher-quality heuristics to documents:
///  - Total word count bounds
///  - Average word length bounds
///  - Symbol-to-word ratio limits
///  - Bullet/ellipsis line ratios
///  - Alphabetic-word ratio
///  - Presence of stop words
pub struct GopherQualityFilter {
    min_doc_words: Option<usize>,
    max_doc_words: Option<usize>,
    min_avg_word_length: Option<f64>,
    max_avg_word_length: Option<f64>,
    max_symbol_word_ratio: Option<f64>,
    max_bullet_lines_ratio: Option<f64>,
    max_ellipsis_lines_ratio: Option<f64>,
    max_non_alpha_words_ratio: Option<f64>,
    min_stop_words: Option<usize>,
    stop_words: HashSet<String>,
}

impl GopherQualityFilter {
    /// Construct a new GopherQualityFilter
    pub fn new(
        min_doc_words: Option<usize>,
        max_doc_words: Option<usize>,
        min_avg_word_length: Option<f64>,
        max_avg_word_length: Option<f64>,
        max_symbol_word_ratio: Option<f64>,
        max_bullet_lines_ratio: Option<f64>,
        max_ellipsis_lines_ratio: Option<f64>,
        max_non_alpha_words_ratio: Option<f64>,
        min_stop_words: Option<usize>,
        stop_words: Option<Vec<String>>,
    ) -> Self {
        let sw = stop_words
            .unwrap_or_else(|| DEFAULT_STOP_WORDS.iter().map(|&s| s.to_string()).collect());
        GopherQualityFilter {
            min_doc_words,
            max_doc_words,
            min_avg_word_length,
            max_avg_word_length,
            max_symbol_word_ratio,
            max_bullet_lines_ratio,
            max_ellipsis_lines_ratio,
            max_non_alpha_words_ratio,
            min_stop_words,
            stop_words: sw.into_iter().collect(),
        }
    }
}

#[async_trait]
impl ProcessingStep for GopherQualityFilter {
    fn name(&self) -> &'static str {
        "GopherQualityFilter"
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        let mut document = document; // Make document mutable
        let text = &document.content;

        let words = split_into_words(text);
        let n_total_words = words.len();
        document.metadata.insert(
            "gopher_quality_metric_total_word_count".to_string(),
            n_total_words.to_string(),
        );

        let non_symbol_words: Vec<&str> = words
            .iter()
            .filter(|w_ref_ref| w_ref_ref.chars().any(|c| !PUNCTUATION.contains(&c)))
            .copied()
            .collect();
        let n_non_symbol_words = non_symbol_words.len();
        document.metadata.insert(
            "gopher_quality_metric_non_symbol_word_count".to_string(),
            n_non_symbol_words.to_string(),
        );

        let avg_word_len_non_symbol = if n_non_symbol_words > 0 {
            let sum_len: usize = non_symbol_words.iter().map(|w| w.chars().count()).sum();
            sum_len as f64 / n_non_symbol_words as f64
        } else {
            0.0
        };
        document.metadata.insert(
            "gopher_quality_metric_avg_word_len_char_non_symbol".to_string(),
            format!("{:.4}", avg_word_len_non_symbol),
        );

        let n_total_words_calc = n_total_words.max(1) as f64; // Denominator for ratios

        let hash_char_count = text.chars().filter(|&c| c == '#').count();
        let hash_char_ratio_vs_total_words = hash_char_count as f64 / n_total_words_calc;
        document.metadata.insert(
            "gopher_quality_metric_hash_char_count".to_string(),
            hash_char_count.to_string(),
        );
        document.metadata.insert(
            "gopher_quality_metric_hash_char_ratio_vs_total_words".to_string(),
            format!("{:.4}", hash_char_ratio_vs_total_words),
        );

        let ellipsis_unit_count = text.matches("...").count() + text.matches("…").count();
        let ellipsis_unit_ratio_vs_total_words = ellipsis_unit_count as f64 / n_total_words_calc;
        document.metadata.insert(
            "gopher_quality_metric_ellipsis_unit_count".to_string(),
            ellipsis_unit_count.to_string(),
        );
        document.metadata.insert(
            "gopher_quality_metric_ellipsis_unit_ratio_vs_total_words".to_string(),
            format!("{:.4}", ellipsis_unit_ratio_vs_total_words),
        );

        let lines: Vec<&str> = text.lines().collect();
        let n_lines_val = lines.len();
        let n_lines_calc = n_lines_val.max(1) as f64; // Denominator for line ratios
        document.metadata.insert(
            "gopher_quality_metric_total_line_count".to_string(),
            n_lines_val.to_string(),
        );

        let bullet_line_count = lines
            .iter()
            .filter(|&&l| {
                let ls = l.trim_start();
                ls.starts_with('•') || ls.starts_with('-') || ls.starts_with('*')
                // Common bullet chars
            })
            .count();
        let bullet_line_ratio = bullet_line_count as f64 / n_lines_calc;
        document.metadata.insert(
            "gopher_quality_metric_bullet_line_count".to_string(),
            bullet_line_count.to_string(),
        );
        document.metadata.insert(
            "gopher_quality_metric_bullet_line_ratio".to_string(),
            format!("{:.4}", bullet_line_ratio),
        );

        let ellipsis_line_count = lines
            .iter()
            .filter(|&&l| {
                let le = l.trim_end();
                le.ends_with("...") || le.ends_with("…")
            })
            .count();
        let ellipsis_line_ratio = ellipsis_line_count as f64 / n_lines_calc;
        document.metadata.insert(
            "gopher_quality_metric_ellipsis_line_count".to_string(),
            ellipsis_line_count.to_string(),
        );
        document.metadata.insert(
            "gopher_quality_metric_ellipsis_line_ratio".to_string(),
            format!("{:.4}", ellipsis_line_ratio),
        );

        let alphabetic_word_count = words
            .iter()
            .filter(|w| w.chars().any(|c| c.is_alphabetic()))
            .count();
        let alphabetic_word_ratio_vs_total_words =
            alphabetic_word_count as f64 / n_total_words_calc;
        document.metadata.insert(
            "gopher_quality_metric_alphabetic_word_count".to_string(),
            alphabetic_word_count.to_string(),
        );
        document.metadata.insert(
            "gopher_quality_metric_alphabetic_word_ratio_vs_total_words".to_string(),
            format!("{:.4}", alphabetic_word_ratio_vs_total_words),
        );

        let stop_word_count = words
            .iter()
            .filter(|w| self.stop_words.contains(w.to_lowercase().as_str()))
            .count();
        document.metadata.insert(
            "gopher_quality_metric_stop_word_count".to_string(),
            stop_word_count.to_string(),
        );

        let mut filter_reasons: Vec<String> = Vec::new();

        // --- Apply Filters ---

        // Word count bounds (based on non-symbol words)
        if let Some(min) = self.min_doc_words {
            if n_non_symbol_words < min {
                filter_reasons.push(format!(
                    "gopher_short_doc ({} non-symbol words, required {})",
                    n_non_symbol_words, min
                ));
            }
        }
        if let Some(max) = self.max_doc_words {
            if n_non_symbol_words > max {
                filter_reasons.push(format!(
                    "gopher_long_doc ({} non-symbol words, max {})",
                    n_non_symbol_words, max
                ));
            }
        }

        // Average word length (based on non-symbol words)
        if let Some(min_avg) = self.min_avg_word_length {
            if avg_word_len_non_symbol < min_avg {
                filter_reasons.push(format!(
                    "gopher_below_avg_threshold (avg len {:.2}, required {:.2}{})",
                    avg_word_len_non_symbol,
                    min_avg,
                    if n_non_symbol_words == 0 && min_avg > 0.0 {
                        " - 0 non-symbol words"
                    } else {
                        ""
                    }
                ));
            }
        }
        if let Some(max_avg) = self.max_avg_word_length {
            // This check is only meaningful if there are words.
            // If n_non_symbol_words is 0, avg_word_len_non_symbol is 0.0, which won't exceed max_avg unless max_avg is negative.
            if n_non_symbol_words > 0 && avg_word_len_non_symbol > max_avg {
                filter_reasons.push(format!(
                    "gopher_above_avg_threshold (avg len {:.2}, max {:.2})",
                    avg_word_len_non_symbol, max_avg
                ));
            }
        }

        // Symbol-to-word ratios
        if let Some(max_sym_ratio) = self.max_symbol_word_ratio {
            if hash_char_ratio_vs_total_words > max_sym_ratio {
                filter_reasons.push(format!(
                    "gopher_too_many_hashes (ratio {:.2}, max {:.2})",
                    hash_char_ratio_vs_total_words, max_sym_ratio
                ));
            }
            // Gopher re-uses max_symbol_word_ratio for ellipsis
            if ellipsis_unit_ratio_vs_total_words > max_sym_ratio {
                filter_reasons.push(format!(
                    "gopher_too_many_ellipsis_units (ratio {:.2}, max {:.2})",
                    ellipsis_unit_ratio_vs_total_words, max_sym_ratio
                ));
            }
        }

        // Line-based bullet/ellipsis ratios
        if let Some(max_bul_ratio) = self.max_bullet_lines_ratio {
            if bullet_line_ratio > max_bul_ratio {
                filter_reasons.push(format!(
                    "gopher_too_many_bullets (ratio {:.2}, max {:.2})",
                    bullet_line_ratio, max_bul_ratio
                ));
            }
        }
        if let Some(max_ell_lines_ratio) = self.max_ellipsis_lines_ratio {
            if ellipsis_line_ratio > max_ell_lines_ratio {
                filter_reasons.push(format!(
                    "gopher_too_many_end_ellipsis_lines (ratio {:.2}, max {:.2})",
                    ellipsis_line_ratio, max_ell_lines_ratio
                ));
            }
        }

        // Alphabetic-word ratio
        if let Some(max_non_alpha_ratio) = self.max_non_alpha_words_ratio {
            let min_required_alpha_ratio = 1.0 - max_non_alpha_ratio;
            if alphabetic_word_ratio_vs_total_words < min_required_alpha_ratio {
                filter_reasons.push(format!(
                    "gopher_below_alpha_threshold (alpha ratio {:.2}, required min {:.2})",
                    alphabetic_word_ratio_vs_total_words, min_required_alpha_ratio
                ));
            }
        }

        // Stop-word presence
        if let Some(min_sw) = self.min_stop_words {
            if min_sw > 0 && stop_word_count < min_sw {
                // Only filter if min_sw > 0
                filter_reasons.push(format!(
                    "gopher_too_few_stop_words (found {}, required {})", // Clarified reason
                    stop_word_count, min_sw
                ));
            }
        }

        if !filter_reasons.is_empty() {
            let reasons_string = filter_reasons.join("; ");
            document.metadata.insert(
                "gopher_quality_filter_status".to_string(),
                "filtered".to_string(),
            );
            document.metadata.insert(
                "gopher_quality_filter_reasons".to_string(),
                reasons_string.clone(),
            );
            Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason: reasons_string,
            })
        } else {
            document.metadata.insert(
                "gopher_quality_filter_status".to_string(),
                "passed".to_string(),
            );
            Ok(document)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::TextDocument;
    use std::collections::HashMap as StdHashMap;

    // Helper to create a TextDocument for tests
    fn create_gopher_test_doc(id: &str, content: &str) -> TextDocument {
        TextDocument {
            id: id.to_string(),
            source: "gopher_test_source".to_string(),
            content: content.to_string(),
            metadata: StdHashMap::new(),
            ..Default::default()
        }
    }

    // Helper for creating a filter with all checks disabled except those being tested
    fn create_permissive_filter() -> GopherQualityFilter {
        GopherQualityFilter::new(None, None, None, None, None, None, None, None, None, None)
    }

    #[tokio::test]
    async fn test_doc_passes_permissive_filter() {
        let filter = create_permissive_filter();
        let doc = create_gopher_test_doc(
            "pass_all",
            "This is a perfectly normal document with the and of words.",
        );
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass permissive filter: {:?}",
            result.err()
        );
    }

    // --- Word Count Tests (non-symbol words) ---
    #[tokio::test]
    async fn test_min_doc_words() {
        let mut filter = create_permissive_filter();
        filter.min_doc_words = Some(3);

        // Passes (3 non-symbol words: "Hello", "world", "test")
        let doc_pass = create_gopher_test_doc("min_words_pass", "Hello world test . !");
        assert!(filter.process(doc_pass).await.is_ok());

        // Fails (2 non-symbol words: "Hello", "world")
        let doc_fail = create_gopher_test_doc("min_words_fail", "Hello world . !");
        let result_fail = filter.process(doc_fail).await;
        assert!(result_fail.is_err());
        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("gopher_short_doc (2 non-symbol words, required 3)"));
            }
            _ => panic!("Expected DocumentFiltered error for short doc"),
        }

        // Fails (0 non-symbol words: content is only symbols)
        let doc_fail_symbols = create_gopher_test_doc("min_words_fail_symbols", ". ! ?");
        let result_fail_symbols = filter.process(doc_fail_symbols).await;
        assert!(result_fail_symbols.is_err());
        match result_fail_symbols.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("gopher_short_doc (0 non-symbol words, required 3)"));
            }
            _ => panic!("Expected DocumentFiltered error for short doc (all symbols)"),
        }
    }

    #[tokio::test]
    async fn test_max_doc_words() {
        let mut filter = create_permissive_filter();
        filter.max_doc_words = Some(3);

        // Passes (3 non-symbol words)
        let doc_pass = create_gopher_test_doc("max_words_pass", "One two three .");
        assert!(filter.process(doc_pass).await.is_ok());

        // Fails (4 non-symbol words)
        let doc_fail = create_gopher_test_doc("max_words_fail", "One two three four .");
        let result_fail = filter.process(doc_fail).await;
        assert!(result_fail.is_err());
        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("gopher_long_doc (4 non-symbol words, max 3)"));
            }
            _ => panic!("Expected DocumentFiltered error for long doc"),
        }
    }

    // --- Average Word Length Tests (non-symbol words, char count) ---
    #[tokio::test]
    async fn test_avg_word_length() {
        // Note: Original test_avg_word_length might have more cases
        let mut filter = create_permissive_filter();
        filter.min_avg_word_length = Some(3.0);
        filter.max_avg_word_length = Some(5.0);

        // Passes (avg len = (3+5+4)/3 = 12/3 = 4.0)
        let doc_pass = create_gopher_test_doc("avg_len_pass", "cat words test .");
        assert!(filter.process(doc_pass).await.is_ok());

        // Fails min_avg_word_length (avg len = (1+2)/2 = 1.5)
        let doc_fail_min = create_gopher_test_doc("avg_len_fail_min", "a it .");
        let result_fail_min = filter.process(doc_fail_min).await;
        assert!(result_fail_min.is_err());
        match result_fail_min.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // For "a it .", non_symbol_words = ["a", "it"]. avg_len = 1.5.
                assert!(
                    reason.contains("gopher_below_avg_threshold (avg len 1.50, required 3.00)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered error for low avg word length"),
        }

        // Fails max_avg_word_length (avg len for "testing", "another" is (7+7)/2 = 7.0)
        let doc_fail_max = create_gopher_test_doc("avg_len_fail_max", "testing another .");
        let result_fail_max = filter.process(doc_fail_max).await;
        assert!(result_fail_max.is_err());
        match result_fail_max.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains("gopher_above_avg_threshold (avg len 7.00, max 5.00)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered error for high avg word length"),
        }

        // Fails because no non-symbol words but min_avg_word_length > 0
        // This is the specific case that was failing: src/pipeline/filters/gopher_quality.rs:456:17
        let doc_fail_no_words = create_gopher_test_doc("avg_len_fail_no_words", ". ! .");
        let result_fail_no_words = filter.process(doc_fail_no_words).await;
        assert!(result_fail_no_words.is_err());
        match result_fail_no_words.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Updated assertion to match the new detailed reason string
                assert!(
                    reason.contains("gopher_below_avg_threshold (avg len 0.00, required 3.00 - 0 non-symbol words)"),
                    "Actual reason: {}", reason
                );
            }
            _ => {
                panic!("Expected DocumentFiltered error for no words with min_avg_word_length > 0")
            }
        }
    }

    // --- Symbol-to-Word Ratio Tests ---
    #[tokio::test]
    async fn test_max_symbol_word_ratio_hashes() {
        let mut filter = create_permissive_filter();
        filter.max_symbol_word_ratio = Some(0.1); // Max 10% hash ratio

        // Passes: 1 hash. `split_into_words` yields 10 words. Ratio 1/10 = 0.1. (0.1 > 0.1) is false.
        let doc_pass_content = "word1 word2 # word3 word4 word5 word6 word7 word8 word9 word10";
        // words from split_into_words: ["word1", "word2", "word3", "word4", "word5", "word6", "word7", "word8", "word9", "word10"] (10 words)
        // hash_count = 1. hash_ratio = 1/10 = 0.1.
        let doc_pass = create_gopher_test_doc("hash_pass", doc_pass_content);
        let result_pass = filter.process(doc_pass.clone()).await; // Use clone if doc is needed later
        assert!(
            result_pass.is_ok(),
            "Hash pass case failed: {:?}. Doc: '{}'",
            result_pass.err(),
            doc_pass_content
        );

        // Fails: 2 hashes. `split_into_words` yields 8 words. Ratio 2/8 = 0.25. (0.25 > 0.1) is true.
        let doc_fail_content = "word1 # word2 # word3 word4 word5 word6 word7 word8";
        // words from split_into_words: ["word1", "word2", "word3", ..., "word8"] (8 words)
        // hash_count = 2. hash_ratio = 2/8 = 0.25
        let doc_fail = create_gopher_test_doc("hash_fail", doc_fail_content);
        let result_fail = filter.process(doc_fail).await;
        assert!(result_fail.is_err());
        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Expected string needs to be precise with {:.2} formatting
                assert!(
                    reason.contains("gopher_too_many_hashes (ratio 0.25, max 0.10)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for too many hashes"),
        }

        let doc_empty = create_gopher_test_doc("hash_empty", "");
        assert!(
            filter.process(doc_empty).await.is_ok(),
            "Empty doc should pass hash ratio"
        );

        let doc_hash_only = create_gopher_test_doc("hash_only_fail", "#");
        // words from split_into_words: [] (0 words). n_total_words_calc = 1.
        // hash_count = 1. hash_ratio = 1/1 = 1.0. (1.0 > 0.1) is true.
        let res_hash_only = filter.process(doc_hash_only).await;
        assert!(res_hash_only.is_err());
        match res_hash_only.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains("gopher_too_many_hashes (ratio 1.00, max 0.10)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for hash only"),
        }
    }

    #[tokio::test]
    async fn test_max_symbol_word_ratio_ellipsis() {
        let mut filter = create_permissive_filter();
        filter.max_symbol_word_ratio = Some(0.1); // Max 10% ellipsis ratio

        // Passes: 1 ellipsis. `split_into_words` yields 10 words. Ratio 1/10 = 0.1. (0.1 > 0.1) is false.
        let doc_pass_content = "word1 word2 ... word3 word4 word5 word6 word7 word8 word9 word10";
        let doc_pass = create_gopher_test_doc("ellipsis_pass", doc_pass_content);
        let result_pass = filter.process(doc_pass.clone()).await;
        assert!(
            result_pass.is_ok(),
            "Ellipsis pass case failed: {:?}. Doc: '{}'",
            result_pass.err(),
            doc_pass_content
        );

        // Fails: 2 ellipses. `split_into_words` yields 8 words. Ratio 2/8 = 0.25. (0.25 > 0.1) is true.
        // This is the failing test: src/pipeline/filters/gopher_quality.rs:554:17
        let doc_fail_content = "word1 ... word2 … word3 word4 word5 word6 word7 word8";
        let doc_fail = create_gopher_test_doc("ellipsis_fail", doc_fail_content);
        let result_fail = filter.process(doc_fail).await;
        assert!(result_fail.is_err());
        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Updated assertion
                assert!(
                    reason.contains("gopher_too_many_ellipsis_units (ratio 0.25, max 0.10)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for too many ellipsis units"),
        }
    }

    // --- Line-based Ratio Tests ---
    #[tokio::test]
    async fn test_max_bullet_lines_ratio() {
        let mut filter = create_permissive_filter();
        filter.max_bullet_lines_ratio = Some(0.5); // Max 50% bullet lines

        // Passes: 2 bullet lines / 4 total lines = 0.5
        let doc_pass_content = "- item 1\n- item 2\nnormal line\nanother normal line";
        let doc_pass = create_gopher_test_doc("bullet_pass", doc_pass_content);
        assert!(filter.process(doc_pass).await.is_ok());

        // Fails: 3 bullet lines / 4 total lines = 0.75
        let doc_fail_content = "- item 1\n- item 2\n- item 3\nnormal line";
        let doc_fail = create_gopher_test_doc("bullet_fail", doc_fail_content);
        let result_fail = filter.process(doc_fail).await;
        assert!(result_fail.is_err());
        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains("gopher_too_many_bullets (ratio 0.75, max 0.50)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for too many bullets"),
        }

        // Edge: 0 lines (empty content) -> 0/1 ratio -> should pass
        let doc_empty = create_gopher_test_doc("bullet_empty", "");
        assert!(filter.process(doc_empty).await.is_ok());

        // Edge: 1 line, 1 bullet line -> 1.0 ratio -> should fail
        let doc_all_bullets = create_gopher_test_doc("bullet_all_bullets", "- all bullets");
        let res_all_bullets = filter.process(doc_all_bullets).await;
        assert!(res_all_bullets.is_err());
        match res_all_bullets.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains("gopher_too_many_bullets (ratio 1.00, max 0.50)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for all bullet lines"),
        }
    }

    #[tokio::test]
    async fn test_max_ellipsis_lines_ratio() {
        let mut filter = create_permissive_filter();
        filter.max_ellipsis_lines_ratio = Some(0.5); // Max 50% lines ending in ellipsis

        // Passes: 2 ellipsis lines / 4 total lines = 0.5
        let doc_pass_content = "Line one...\nLine two…\nNormal line\nAnother normal";
        let doc_pass = create_gopher_test_doc("ell_lines_pass", doc_pass_content);
        assert!(filter.process(doc_pass).await.is_ok());

        // Fails: 3 ellipsis lines / 4 total lines = 0.75
        // This is the failing test: src/pipeline/filters/gopher_quality.rs:628:17
        let doc_fail_content = "Line one...\nLine two…\nLine three...\nNormal line";
        let doc_fail = create_gopher_test_doc("ell_lines_fail", doc_fail_content);
        let result_fail = filter.process(doc_fail).await;
        assert!(result_fail.is_err());
        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Updated assertion
                assert!(
                    reason.contains("gopher_too_many_end_ellipsis_lines (ratio 0.75, max 0.50)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for too many ellipsis lines"),
        }
    }

    // --- Alphabetic Word Ratio Test ---
    #[tokio::test]
    async fn test_alphabetic_word_ratio() {
        let mut filter = create_permissive_filter();
        filter.max_non_alpha_words_ratio = Some(0.5); // Min 50% alpha words

        // doc_pass: content "word 123 word !!!"
        // split_into_words (after filtering punctuation-only tokens) -> ["word", "123", "word"]. n_total_words = 3.
        // alpha_words -> ["word", "word"]. alpha_word_count = 2.
        // alpha_ratio = 2.0/3.0 = 0.666...
        // required_min_alpha = 1.0 - 0.5 = 0.5.
        // 0.666... < 0.5 is false. Should pass.
        let doc_pass = create_gopher_test_doc("alpha_pass", "word 123 word !!!");
        let result_pass = filter.process(doc_pass.clone()).await;
        assert!(
            result_pass.is_ok(),
            "Pass case failed for 'alpha_pass'. Expected Ok, got Err({:?})",
            result_pass.err()
        );

        // doc_fail: content "word 123 456 !!!"
        // split_into_words -> ["word", "123", "456"]. n_total_words = 3.
        // alpha_words -> ["word"]. alpha_word_count = 1.
        // alpha_ratio = 1.0/3.0 = 0.333...
        // required_min_alpha = 0.5.
        // 0.333... < 0.5 is true. Should fail.
        let doc_fail = create_gopher_test_doc("alpha_fail", "word 123 456 !!!");
        let result_fail = filter.process(doc_fail).await;
        assert!(
            result_fail.is_err(),
            "Fail case 'alpha_fail' should have returned Err, but got Ok"
        );

        let expected_alpha_ratio_val = 1.0 / 3.0; // 0.333...
        let expected_required_min_val = 1.0 - 0.5; // 0.5
        let expected_reason_substring = format!(
            "gopher_below_alpha_threshold (alpha ratio {:.2}, required min {:.2})",
            expected_alpha_ratio_val, expected_required_min_val
        ); // Should be "gopher_below_alpha_threshold (alpha ratio 0.33, required min 0.50)"

        match result_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains(&expected_reason_substring),
                    "Actual reason: '{}', Expected substring: '{}'",
                    reason,
                    expected_reason_substring
                );
            }
            _ => panic!("Expected DocumentFiltered for low alpha ratio"),
        }

        // doc_all_non_alpha: content "123 456 789 !!!"
        // split_into_words -> ["123", "456", "789"]. n_total_words = 3.
        // alpha_words -> []. alpha_word_count = 0.
        // alpha_ratio = 0.0/3.0 = 0.0.
        // required_min_alpha = 0.5.
        // 0.0 < 0.5 is true. Should fail.
        let doc_all_non_alpha = create_gopher_test_doc("alpha_all_non_alpha", "123 456 789 !!!");
        let result_all_non_alpha = filter.process(doc_all_non_alpha).await;
        assert!(
            result_all_non_alpha.is_err(),
            "Fail case 'alpha_all_non_alpha' should have returned Err, got Ok"
        );

        let expected_alpha_ratio_val_2 = 0.0 / 3.0; // n_total_words is 3, not 4
        let expected_reason_substring_2 = format!(
            "gopher_below_alpha_threshold (alpha ratio {:.2}, required min {:.2})",
            expected_alpha_ratio_val_2, expected_required_min_val
        ); // Should be "gopher_below_alpha_threshold (alpha ratio 0.00, required min 0.50)"
        match result_all_non_alpha.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains(&expected_reason_substring_2),
                    "Actual reason: '{}', Expected substring: '{}'",
                    reason,
                    expected_reason_substring_2
                );
            }
            _ => panic!("Expected DocumentFiltered for all non-alpha"),
        }

        // doc_empty: content ""
        // split_into_words -> []. n_total_words = 0. n_total_words_calc = 1.
        // alpha_words -> []. alpha_word_count = 0.
        // alpha_ratio = 0.0/1.0 = 0.0.
        // required_min_alpha = 0.5.
        // 0.0 < 0.5 is true. Should fail.
        let doc_empty = create_gopher_test_doc("alpha_empty_fail", "");
        let res_empty = filter.process(doc_empty).await;
        assert!(
            res_empty.is_err(),
            "Fail case 'alpha_empty_fail' should have returned Err, got Ok"
        );
        let expected_alpha_ratio_val_3 = 0.0 / 1.0; // n_total_words_calc is 1
        let expected_reason_substring_3 = format!(
            "gopher_below_alpha_threshold (alpha ratio {:.2}, required min {:.2})",
            expected_alpha_ratio_val_3, expected_required_min_val
        ); // Should be "gopher_below_alpha_threshold (alpha ratio 0.00, required min 0.50)"
        match res_empty.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains(&expected_reason_substring_3),
                    "Actual reason: '{}', Expected substring: '{}'",
                    reason,
                    expected_reason_substring_3
                );
            }
            _ => panic!("Expected DocumentFiltered for empty doc and alpha ratio"),
        }
    }

    // --- Stop Word Presence Test ---
    // --- Stop Word Presence Test ---
    #[tokio::test]
    async fn test_stop_word_presence() {
        // Replaces test_stop_word_presence_clarified_reason
        let mut filter_default_sw = create_permissive_filter();
        filter_default_sw.min_stop_words = Some(2); // Requires at least 2 stop words from default list

        let doc_pass_default =
            create_gopher_test_doc("sw_pass_default", "the quick brown fox and the lazy dog");
        assert!(filter_default_sw.process(doc_pass_default).await.is_ok());

        // This is the failing test: src/pipeline/filters/gopher_quality.rs:769:17
        let doc_fail_default =
            create_gopher_test_doc("sw_fail_default", "a quick brown fox is lazy");
        let result_fail_default = filter_default_sw.process(doc_fail_default).await;
        assert!(result_fail_default.is_err());
        match result_fail_default.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Updated assertion
                assert!(
                    reason.contains("gopher_too_few_stop_words (found 0, required 2)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for too few default stop words"),
        }

        // Test with custom stop words
        let custom_sw = vec!["custom".to_string(), "words".to_string()];
        let filter_custom_sw = GopherQualityFilter::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1),
            Some(custom_sw),
        );

        let doc_pass_custom =
            create_gopher_test_doc("sw_pass_custom", "this is a custom test with other words");
        assert!(filter_custom_sw.process(doc_pass_custom).await.is_ok());

        let doc_fail_custom =
            create_gopher_test_doc("sw_fail_custom", "this is a regular sentence");
        let result_fail_custom = filter_custom_sw.process(doc_fail_custom).await;
        assert!(result_fail_custom.is_err());
        match result_fail_custom.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Updated assertion
                assert!(
                    reason.contains("gopher_too_few_stop_words (found 0, required 1)"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for too few custom stop words"),
        }

        let mut filter_no_sw_needed = create_permissive_filter();
        filter_no_sw_needed.min_stop_words = Some(0);
        let doc_no_sw = create_gopher_test_doc("sw_zero_needed", "no stop words here");
        assert!(filter_no_sw_needed.process(doc_no_sw.clone()).await.is_ok());

        filter_no_sw_needed.min_stop_words = None;
        assert!(filter_no_sw_needed.process(doc_no_sw).await.is_ok());
    }
}
