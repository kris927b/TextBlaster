use async_trait::async_trait;
use regex::Regex;
use std::collections::{HashMap, HashSet};

use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result}; // Use your Result type alias
use crate::executor::ProcessingStep; // Assuming ProcessingStep trait is here or imported
use crate::utils::text::split_into_words;

/// Generate all contiguous n-grams of words, joined by spaces.
fn get_n_grams(words: &[&str], n: usize) -> Vec<String> {
    // if n == 0 { return Vec::new(); } // Original guard
    // words.windows(n).map(|window| window.join(" ")).collect()

    // Try wrapping the call instead of returning early
    if n > 0 {
        words.windows(n).map(|window| window.join(" ")).collect()
    } else {
        Vec::new() // Return empty vec if n is 0
    }
}

/// Count duplicate elements and the total length of duplicate elements in characters.
fn find_duplicates(items: &[String]) -> (usize, usize) {
    let mut unique = HashSet::new();
    let mut dup_chars = 0;
    let mut dup_elems = 0;
    for elem in items {
        if !unique.insert(elem.clone()) {
            dup_chars += elem.len();
            dup_elems += 1;
        }
    }
    (dup_elems, dup_chars)
}

/// Find the most frequent element and return its length multiplied by its count.
fn find_top_duplicate(items: &[String]) -> usize {
    if items.is_empty() {
        return 0;
    }
    let mut counter: HashMap<String, usize> = HashMap::new();
    for elem in items {
        *counter.entry(elem.to_string()).or_insert(0) += 1;
    }

    let max_count = counter.values().max().copied().unwrap_or(0);

    if max_count <= 1 {
        return 0; // No duplicates found
    }

    // Find the maximum length contribution (len * count) among all items with the max_count.
    let mut max_len_contribution = 0;
    for (gram_str, count) in counter.iter() {
        if *count == max_count {
            let current_contribution = gram_str.len() * max_count;
            if current_contribution > max_len_contribution {
                max_len_contribution = current_contribution;
            }
        }
    }

    max_len_contribution
}

/// Slide over words in steps, summing lengths of duplicate n-grams (concatenated without spaces).
fn find_all_duplicate(words: &[&str], n: usize) -> usize {
    if n == 0 || words.len() < n {
        return 0;
    }
    let mut unique = HashSet::new();
    let mut repeated_chars = 0;
    let mut idx = 0;
    let n_words = words.len();
    while idx + n <= n_words {
        let n_gram = words[idx..idx + n].concat();
        if !unique.insert(n_gram.clone()) {
            repeated_chars += n_gram.len();
            idx += n;
        } else {
            idx += 1;
        }
    }
    repeated_chars
}

/// A filter that detects repeated lines, paragraphs, and n-grams in a document.
pub struct GopherRepetitionFilter {
    dup_line_frac: Option<f64>,
    dup_para_frac: Option<f64>,
    dup_line_char_frac: Option<f64>,
    dup_para_char_frac: Option<f64>,
    top_n_grams: Vec<(usize, f64)>,
    dup_n_grams: Vec<(usize, f64)>,
    paragraph_exp: Regex,
    line_splitter: Regex,
}

impl GopherRepetitionFilter {
    /// Create a new GopherRepetitionFilter with specified thresholds.
    pub fn new(
        dup_line_frac: Option<f64>,
        dup_para_frac: Option<f64>,
        dup_line_char_frac: Option<f64>,
        dup_para_char_frac: Option<f64>,
        top_n_grams: Vec<(usize, f64)>,
        dup_n_grams: Vec<(usize, f64)>,
    ) -> Self {
        GopherRepetitionFilter {
            dup_line_frac,
            dup_para_frac,
            dup_line_char_frac,
            dup_para_char_frac,
            top_n_grams,
            dup_n_grams,
            paragraph_exp: Regex::new(r"\n{2,}").unwrap(),
            line_splitter: Regex::new(r"\n+").unwrap(),
        }
    }
}

#[async_trait]
impl ProcessingStep for GopherRepetitionFilter {
    fn name(&self) -> &'static str {
        "GopherRepetitionFilter"
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        let mut document = document; // Make document mutable to modify metadata

        let text_content = &document.content;
        // Base character length on trimmed text for consistency with paragraph/line/word splitting
        let trimmed_text = text_content.trim();
        let text_char_len = trimmed_text.chars().count().max(1) as f64;

        if trimmed_text.is_empty() {
            document.metadata.insert(
                "gopher_repetition_filter_status".to_string(),
                "skipped_empty_content".to_string(),
            );
            // Add 0.0 for all potential ratios for consistency if downstream expects them
            // For thresholds that are Some (i.e., active)
            if self.dup_para_frac.is_some() {
                document.metadata.insert(
                    "gopher_dup_para_elems_ratio".to_string(),
                    "0.0000".to_string(),
                );
            }
            if self.dup_para_char_frac.is_some() {
                document.metadata.insert(
                    "gopher_dup_para_chars_ratio".to_string(),
                    "0.0000".to_string(),
                );
            }
            if self.dup_line_frac.is_some() {
                document.metadata.insert(
                    "gopher_dup_line_elems_ratio".to_string(),
                    "0.0000".to_string(),
                );
            }
            if self.dup_line_char_frac.is_some() {
                document.metadata.insert(
                    "gopher_dup_line_chars_ratio".to_string(),
                    "0.0000".to_string(),
                );
            }
            // For configured n-grams
            for &(n, _) in &self.top_n_grams {
                document.metadata.insert(
                    format!("gopher_top_{}_gram_char_ratio", n),
                    "0.0000".to_string(),
                );
            }
            for &(n, _) in &self.dup_n_grams {
                document.metadata.insert(
                    format!("gopher_all_dup_{}_gram_char_ratio", n),
                    "0.0000".to_string(),
                );
            }
            return Ok(document);
        }

        let mut filter_reasons: Vec<String> = Vec::new();

        // Paragraph checks
        // self.paragraph_exp.split() will yield at least one item if trimmed_text is not empty.
        let paragraphs: Vec<String> = self
            .paragraph_exp
            .split(trimmed_text)
            .map(str::to_string)
            .collect();

        let (para_dup_elems, para_dup_chars) = find_duplicates(&paragraphs);
        // paragraphs.len() will be >= 1 if trimmed_text is not empty. Use .max(1) as a safeguard.
        let para_len = paragraphs.len().max(1) as f64;

        let ratio_para_dup_elems = para_dup_elems as f64 / para_len;
        document.metadata.insert(
            "gopher_dup_para_elems_ratio".to_string(),
            format!("{:.4}", ratio_para_dup_elems),
        );
        if let Some(threshold) = self.dup_para_frac {
            if ratio_para_dup_elems > threshold {
                filter_reasons.push(format!(
                    "dup_para_frac (ratio {:.2}, max {:.2})",
                    ratio_para_dup_elems, threshold
                ));
            }
        }

        let ratio_para_dup_chars = para_dup_chars as f64 / text_char_len;
        document.metadata.insert(
            "gopher_dup_para_chars_ratio".to_string(),
            format!("{:.4}", ratio_para_dup_chars),
        );
        if let Some(threshold) = self.dup_para_char_frac {
            if ratio_para_dup_chars > threshold {
                filter_reasons.push(format!(
                    "dup_para_char_frac (ratio {:.2}, max {:.2})",
                    ratio_para_dup_chars, threshold
                ));
            }
        }

        // Line checks
        // self.line_splitter.split() will yield at least one item if trimmed_text is not empty.
        let lines: Vec<String> = self
            .line_splitter
            .split(trimmed_text)
            .map(str::to_string)
            .collect();

        let (line_dup_elems, line_dup_chars) = find_duplicates(&lines);
        // lines.len() will be >= 1 if trimmed_text is not empty. Use .max(1) as a safeguard.
        let line_len = lines.len().max(1) as f64;

        let ratio_line_dup_elems = line_dup_elems as f64 / line_len;
        document.metadata.insert(
            "gopher_dup_line_elems_ratio".to_string(),
            format!("{:.4}", ratio_line_dup_elems),
        );
        if let Some(threshold) = self.dup_line_frac {
            if ratio_line_dup_elems > threshold {
                filter_reasons.push(format!(
                    "dup_line_frac (ratio {:.2}, max {:.2})",
                    ratio_line_dup_elems, threshold
                ));
            }
        }

        let ratio_line_dup_chars = line_dup_chars as f64 / text_char_len;
        document.metadata.insert(
            "gopher_dup_line_chars_ratio".to_string(),
            format!("{:.4}", ratio_line_dup_chars),
        );
        if let Some(threshold) = self.dup_line_char_frac {
            if ratio_line_dup_chars > threshold {
                filter_reasons.push(format!(
                    "dup_line_char_frac (ratio {:.2}, max {:.2})",
                    ratio_line_dup_chars, threshold
                ));
            }
        }

        // Word-based n-grams
        let words = split_into_words(trimmed_text);
        // Helper functions (get_n_grams, find_top_duplicate, find_all_duplicate)
        // handle empty `words` or `n=0` correctly, typically resulting in 0.0 ratios.

        for &(n, frac_threshold) in &self.top_n_grams {
            let n_grams = get_n_grams(&words, n);
            let top_len_contribution = find_top_duplicate(&n_grams);
            let ratio_top_n_gram_chars = top_len_contribution as f64 / text_char_len;

            document.metadata.insert(
                format!("gopher_top_{}_gram_char_ratio", n),
                format!("{:.4}", ratio_top_n_gram_chars),
            );
            // Only apply filter if n > 0, as n=0 calculations result in 0 and thresholds are not meaningful.
            if n > 0 && ratio_top_n_gram_chars > frac_threshold {
                filter_reasons.push(format!(
                    "top_{}_gram (ratio {:.2}, max {:.2})",
                    n, ratio_top_n_gram_chars, frac_threshold
                ));
            }
        }

        for &(n, frac_threshold) in &self.dup_n_grams {
            let dup_n_gram_chars = find_all_duplicate(&words, n);
            let ratio_all_dup_n_gram_chars = dup_n_gram_chars as f64 / text_char_len;

            document.metadata.insert(
                format!("gopher_all_dup_{}_gram_char_ratio", n),
                format!("{:.4}", ratio_all_dup_n_gram_chars),
            );
            // Only apply filter if n > 0.
            if n > 0 && ratio_all_dup_n_gram_chars > frac_threshold {
                filter_reasons.push(format!(
                    "duplicated_{}_n_grams (ratio {:.2}, max {:.2})",
                    n, ratio_all_dup_n_gram_chars, frac_threshold
                ));
            }
        }

        if !filter_reasons.is_empty() {
            document.metadata.insert(
                "gopher_repetition_filter_status".to_string(),
                "filtered".to_string(),
            );
            let reasons_string = filter_reasons.join("; ");
            document.metadata.insert(
                "gopher_repetition_filter_reasons".to_string(),
                reasons_string.clone(),
            );

            return Err(PipelineError::DocumentFiltered {
                document: document,
                reason: reasons_string,
            });
        }

        document.metadata.insert(
            "gopher_repetition_filter_status".to_string(),
            "passed".to_string(),
        );
        Ok(document)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Minimal main TextDocument for filter tests
    use crate::data_model::TextDocument as MainTextDocument;
    use std::collections::HashMap as StdHashMap;

    // ... (helper functions create_rep_test_doc, create_rep_permissive_filter) ...
    fn create_rep_test_doc(id: &str, content: &str) -> MainTextDocument {
        MainTextDocument {
            id: id.to_string(),
            source: "rep_test_source".to_string(),
            content: content.to_string(),
            metadata: StdHashMap::new(),
        }
    }

    fn create_rep_permissive_filter() -> GopherRepetitionFilter {
        GopherRepetitionFilter::new(None, None, None, None, Vec::new(), Vec::new())
    }

    // --- Tests for Helper Functions ---
    #[test]
    fn test_get_n_grams_logic() {
        let words = vec!["a", "b", "c", "d"];
        assert_eq!(
            get_n_grams(&words, 2),
            vec!["a b".to_string(), "b c".to_string(), "c d".to_string()]
        );
        assert_eq!(
            get_n_grams(&words, 1),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string()
            ]
        );
        assert_eq!(get_n_grams(&words, 4), vec!["a b c d".to_string()]);
        assert_eq!(get_n_grams(&words, 5), Vec::<String>::new());
        assert_eq!(get_n_grams(&[], 2), Vec::<String>::new());
        assert_eq!(
            get_n_grams(&words, 0),
            Vec::<String>::new(),
            "N-grams of 0 should be empty"
        );
    }

    #[test]
    fn test_find_duplicates_logic() {
        assert_eq!(
            find_duplicates(&vec!["a".to_string(), "b".to_string(), "c".to_string()]),
            (0, 0)
        ); // no duplicates
        assert_eq!(
            find_duplicates(&vec!["a".to_string(), "b".to_string(), "a".to_string()]),
            (1, 1)
        ); // "a" is dup, len 1
        assert_eq!(
            find_duplicates(&vec![
                "ab".to_string(),
                "cd".to_string(),
                "ab".to_string(),
                "ef".to_string(),
                "cd".to_string()
            ]),
            (2, 4)
        ); // "ab", "cd" are dups
        assert_eq!(
            find_duplicates(&vec!["a".to_string(), "a".to_string(), "a".to_string()]),
            (2, 2)
        ); // two "a"s are dups
        assert_eq!(find_duplicates(&Vec::<String>::new()), (0, 0));
    }

    #[test]
    fn test_find_top_duplicate_logic() {
        assert_eq!(
            find_top_duplicate(&vec!["a".to_string(), "a".to_string()]),
            2,
            "Test case: [a, a]"
        );
        assert_eq!(
            find_top_duplicate(&vec![
                "a".to_string(),
                "a".to_string(),
                "b".to_string(),
                "b".to_string()
            ]),
            2,
            "Test case: [a, a, b, b]"
        );
        assert_eq!(
            find_top_duplicate(&vec![
                "a b".to_string(),
                "c d".to_string(),
                "a b".to_string()
            ]),
            6,
            "Test case: [ab, cd, ab]"
        );
        assert_eq!(
            find_top_duplicate(&vec!["a".to_string(), "b".to_string(), "c".to_string()]),
            0,
            "Test case: [a, b, c]"
        );
        assert_eq!(
            find_top_duplicate(&vec![
                "aa".to_string(),
                "aa".to_string(),
                "b".to_string(),
                "b".to_string()
            ]),
            4,
            "Test case: [aa, aa, b, b]"
        );
        assert_eq!(
            find_top_duplicate(&vec!["a".to_string(), "a".to_string(), "a".to_string()]),
            3,
            "Test case: [a, a, a]"
        );
        assert_eq!(
            find_top_duplicate(&Vec::<String>::new()),
            0,
            "Test case: []"
        );
        assert_eq!(
            find_top_duplicate(&vec!["unique".to_string()]),
            0,
            "Test case: [unique]"
        );
    }

    #[test]
    fn test_find_all_duplicate_no_dups() {
        let words_no_dups = vec!["a", "b", "c", "d", "e"];
        assert_eq!(
            find_all_duplicate(&words_no_dups, 2),
            0,
            "Case: no duplicates n=2"
        );
        assert_eq!(
            find_all_duplicate(&words_no_dups, 3),
            0,
            "Case: no duplicates n=3"
        );
    }

    #[test]
    fn test_find_all_duplicate_simple_dups() {
        let words1 = vec!["a", "b", "c", "a", "b", "d"];
        assert_eq!(find_all_duplicate(&words1, 2), 2, "Case: words1");
    }

    #[test]
    fn test_find_all_duplicate_multiple_dups() {
        let words2 = vec!["a", "b", "a", "b", "a", "b"];
        assert_eq!(find_all_duplicate(&words2, 2), 4, "Case: words2");
    }

    #[test]
    fn test_find_all_duplicate_repeated_single_char_ngram() {
        let words3 = vec!["a", "a", "a", "a", "a"];
        assert_eq!(
            find_all_duplicate(&words3, 2),
            4,
            "Case: words3 [a,a,a,a,a] n=2"
        );
    }

    #[test]
    fn test_find_all_duplicate_edge_cases() {
        let words_no_dups = vec!["a", "b", "c", "d", "e"];
        assert_eq!(find_all_duplicate(&[], 2), 0, "Case: empty slice");
        assert_eq!(
            find_all_duplicate(&words_no_dups, 0),
            0,
            "n=0 should yield 0"
        );
        assert_eq!(
            find_all_duplicate(&words_no_dups, 6),
            0,
            "n > words.len() should yield 0"
        );
    }

    // --- Tests for GopherRepetitionFilter Process Method ---
    #[tokio::test]
    async fn test_rep_filter_passes_permissive() {
        let filter = create_rep_permissive_filter();
        let doc = create_rep_test_doc(
            "pass_rep",
            "This is a normal document.\nIt has multiple lines.\n\nAnd multiple paragraphs.",
        );
        assert!(filter.process(doc).await.is_ok());
    }

    #[tokio::test]
    async fn test_rep_filter_empty_doc_passes() {
        let filter = GopherRepetitionFilter::new(
            Some(0.1),
            Some(0.1),
            Some(0.1),
            Some(0.1),
            vec![(2, 0.1)],
            vec![(2, 0.1)],
        );
        let doc = create_rep_test_doc("empty_rep", "");
        assert!(
            filter.process(doc).await.is_ok(),
            "Empty doc should pass all repetition checks"
        );
        let doc_whitespace = create_rep_test_doc("whitespace_rep", "   \n\n   ");
        assert!(
            filter.process(doc_whitespace).await.is_ok(),
            "Whitespace doc should pass all repetition checks"
        );
    }

    // --- Paragraph Repetition Tests ---
    #[tokio::test]
    async fn test_duplicate_paragraphs() {
        let para1 = "This is the first paragraph.";
        let para2 = "This is the second paragraph.";
        let content_pass = format!("{}\n\n{}\n\nAnother unique.", para1, para2); // 3 unique paras
        let content_fail_frac = format!("{}\n\n{}\n\n{}", para1, para2, para1); // 3 paras, 1 dup ("para1") -> frac=1/3=0.333
        let content_fail_char_frac = format!("{}\n\n{}\n\n{}", para1, para1, para1); // 3 paras, 2 dups ("para1") -> dup_chars=2*para1.len()

        // Test Fraction
        let mut filter_frac = create_rep_permissive_filter();
        filter_frac.dup_para_frac = Some(0.3); // Max 30% duplicate paragraphs

        assert!(filter_frac
            .process(create_rep_test_doc("para_pass_frac", &content_pass))
            .await
            .is_ok());

        let res_fail_frac = filter_frac
            .process(create_rep_test_doc("para_fail_frac", &content_fail_frac))
            .await;
        assert!(res_fail_frac.is_err());
        match res_fail_frac.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("dup_para_frac (ratio 0.33, max 0.30)"))
            }
            _ => panic!("Expected DocumentFiltered"),
        }

        // Test Char Fraction
        let mut filter_char_frac = create_rep_permissive_filter();
        let total_chars = content_fail_char_frac.chars().count() as f64;
        let dup_chars = (2 * para1.chars().count()) as f64;
        let expected_ratio = dup_chars / total_chars; // Calculate expected ratio
        filter_char_frac.dup_para_char_frac = Some(expected_ratio - 0.01); // Set threshold just below actual ratio

        assert!(filter_char_frac
            .process(create_rep_test_doc("para_pass_char_frac", &content_pass))
            .await
            .is_ok());

        let res_fail_char_frac = filter_char_frac
            .process(create_rep_test_doc(
                "para_fail_char_frac",
                &content_fail_char_frac,
            ))
            .await;
        assert!(res_fail_char_frac.is_err());
        match res_fail_char_frac.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("dup_para_char_frac"))
            } // Check reason type
            _ => panic!("Expected DocumentFiltered"),
        }
    }

    // --- Line Repetition Tests ---
    #[tokio::test]
    async fn test_duplicate_lines() {
        let line1 = "This is line one.";
        let line2 = "This is line two.";
        let content_pass = format!("{}\n{}\nUnique line", line1, line2); // 3 unique lines
        let content_fail_frac = format!("{}\n{}\n{}", line1, line2, line1); // 3 lines, 1 dup ("line1") -> frac=1/3=0.333
        let content_fail_char_frac = format!("{}\n{}\n{}", line1, line1, line1); // 3 lines, 2 dups ("line1") -> dup_chars=2*line1.len()

        // Test Fraction
        let threshold_frac = 0.3;
        let mut filter_frac = create_rep_permissive_filter();
        filter_frac.dup_line_frac = Some(threshold_frac); // Max 30% duplicate lines

        assert!(filter_frac
            .process(create_rep_test_doc("line_pass_frac", &content_pass))
            .await
            .is_ok());

        let res_fail_frac = filter_frac
            .process(create_rep_test_doc("line_fail_frac", &content_fail_frac))
            .await;
        assert!(res_fail_frac.is_err());

        let expected_ratio_frac = 1.0 / 3.0; // ~0.333
        let expected_reason_substring_frac = format!(
            "dup_line_frac (ratio {:.2}, max {:.2})",
            expected_ratio_frac, threshold_frac
        ); // Should be "dup_line_frac (ratio 0.33, max 0.30)"

        match res_fail_frac.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains(&expected_reason_substring_frac),
                    "Actual reason: '{}', Expected substring: '{}'",
                    reason,
                    expected_reason_substring_frac
                );
            }
            _ => panic!("Expected DocumentFiltered"),
        }

        // Test Char Fraction
        let threshold_char_frac_delta = 0.01;
        let total_chars = content_fail_char_frac.chars().count() as f64;
        let dup_chars = (2 * line1.chars().count()) as f64;
        let actual_char_ratio = dup_chars / total_chars.max(1.0); // Use max(1.0) to avoid NaN for empty string case
        let threshold_char_frac = actual_char_ratio - threshold_char_frac_delta; // Set threshold just below actual ratio

        let mut filter_char_frac = create_rep_permissive_filter();
        filter_char_frac.dup_line_char_frac = Some(threshold_char_frac);

        assert!(filter_char_frac
            .process(create_rep_test_doc("line_pass_char_frac", &content_pass))
            .await
            .is_ok());

        let res_fail_char_frac = filter_char_frac
            .process(create_rep_test_doc(
                "line_fail_char_frac",
                &content_fail_char_frac,
            ))
            .await;
        assert!(res_fail_char_frac.is_err());

        // Prefix the unused variable with an underscore
        let _expected_reason_substring_char_frac = format!(
            "dup_line_char_frac (ratio {:.2}, max {:.2})",
            actual_char_ratio, threshold_char_frac
        );

        match res_fail_char_frac.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.starts_with("dup_line_char_frac (ratio"),
                    "Reason should start correctly. Got: {}",
                    reason
                );
                assert!(
                    reason.contains(&format!("max {:.2}", threshold_char_frac)),
                    "Reason max incorrect. Got: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered"),
        }
    }

    // --- N-Gram Repetition Tests ---
    #[tokio::test]
    async fn test_top_n_grams() {
        let mut filter = create_rep_permissive_filter();
        // Check 2-grams, max 30% char fraction for the top one
        filter.top_n_grams = vec![(2, 0.3)];

        // Content: "a b c d e f a b g h i j"
        // Words: ["a", "b", "c", "d", "e", "f", "a", "b", "g", "h", "i", "j"] (12 words)
        // 2-grams: ["a b", "b c", "c d", "d e", "e f", "f a", "a b", "b g", "g h", "h i", "i j"] (11 2-grams)
        // Frequencies: {"a b": 2, others: 1}. Top is "a b".
        // Contribution: len("a b") * 2 = 3 * 2 = 6 chars.
        // Total text chars (approx): 23.
        // Ratio = 6 / 23 = ~0.26. (0.26 <= 0.3). Should pass.
        let content_pass = "a b c d e f a b g h i j";
        assert!(filter
            .process(create_rep_test_doc("top_ngram_pass", content_pass))
            .await
            .is_ok());

        // Content: "a b c a b d a b e a b"
        // Words: ["a", "b", "c", "a", "b", "d", "a", "b", "e", "a", "b"] (11 words)
        // 2-grams: ["a b", "b c", "c a", "a b", "b d", "d a", "a b", "b e", "e a", "a b"] (10 2-grams)
        // Frequencies: {"a b": 4, others: 1}. Top is "a b".
        // Contribution: len("a b") * 4 = 3 * 4 = 12 chars.
        // Total text chars (approx): 21.
        // Ratio = 12 / 21 = ~0.57. (0.57 > 0.3). Should fail.
        let content_fail = "a b c a b d a b e a b";
        let res_fail = filter
            .process(create_rep_test_doc("top_ngram_fail", content_fail))
            .await;
        assert!(res_fail.is_err());
        match res_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("top_2_gram"))
            } // Check reason type
            _ => panic!("Expected DocumentFiltered"),
        }
    }

    #[tokio::test]
    async fn test_duplicate_n_grams() {
        let mut filter = create_rep_permissive_filter();
        // Check 2-grams, max 10% total duplicate char fraction
        filter.dup_n_grams = vec![(2, 0.1)];

        // Content: "a b c d e a b f g"
        // Words: ["a", "b", "c", "d", "e", "a", "b", "f", "g"] (9 words)
        // `find_all_duplicate(words, 2)`:
        // idx=0: "a b" new. idx=1.
        // idx=1: "b c" new. idx=2.
        // idx=2: "c d" new. idx=3.
        // idx=3: "d e" new. idx=4.
        // idx=4: "e a" new. idx=5.
        // idx=5: "a b" DUP. rep_chars=len("a")+len("b")=2. idx=5+2=7.
        // idx=7: "f g" new. idx=8.
        // End. Total dup_n_gram_chars = 2.
        // Total text chars = 17.
        // Ratio = 2 / 17 = ~0.117. (0.117 > 0.1). Should fail.
        let content_fail = "a b c d e a b f g";
        let res_fail = filter
            .process(create_rep_test_doc("dup_ngram_fail", content_fail))
            .await;
        assert!(res_fail.is_err());
        match res_fail.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("duplicated_2_n_grams"))
            } // Check reason type
            _ => panic!("Expected DocumentFiltered"),
        }

        // Content: "a b c d e f g h i" (no dups)
        let content_pass = "a b c d e f g h i";
        assert!(filter
            .process(create_rep_test_doc("dup_ngram_pass", content_pass))
            .await
            .is_ok());
    }
}
