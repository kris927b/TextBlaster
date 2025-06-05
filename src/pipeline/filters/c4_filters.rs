use crate::config::C4BadWordsParams; // New import
use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::executor::ProcessingStep;
use crate::utils::text::{split_into_sentences, split_into_words};

use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::rngs::StdRng; // For random number generation
use rand::{Rng, SeedableRng}; // For random number generation
use regex::Regex;
use std::collections::HashMap; // May already exist, ensure it's there

// Constants based on C4 and reference implementation
const END_PUNCTUATION: [char; 4] = ['.', '!', '?', '"'];
const ELLIPSIS: &str = "...";

lazy_static! {
    static ref POLICY_SUBSTRINGS: Vec<&'static str> = vec![
        "cookie",
        "cookies",
        "terms of use",
        "privacy policy",
        "gdpr",
        "ccpa",
        "california consumer privacy act",
        "do not sell my personal information",
    ];
    // Simple regex for Wikipedia-style citations like [1], [2, 3], [45]
    static ref CITATION_REGEX: Regex = Regex::new(r"\[\d+(?:,\s*\d+)*\]").expect("Invalid regex");
}

/// Applies heuristic rules from C4 https://jmlr.org/papers/volume21/20-074/20-074.pdf
///
/// - We only retained lines that ended in a terminal punctuation mark (! . " ?)
/// - We discarded any page with fewer than 5 sentences and only retained lines that contained at least 3 words
/// - [NOT IMPLEMENTED IN RUST] We removed any page that contained any word on the “List of Dirty, Naughty, Obscene or Otherwise Bad Words”
/// - We removed any line with the word Javascript.
/// - We removed any page where the phrase “lorem ipsum” appeared
/// - We removed any pages that contained a curly bracket
///   Additional filters not mentioned on the list from the paper but on the code:
/// - Remove lines with one word over 1000 chars
/// - Remove lines with cookies and terms of use keywords
/// - Remove wikipedia style citations from the text
pub struct C4QualityFilter {
    split_paragraph: bool, // In Rust, we'll primarily use line-by-line as default
    remove_citations: bool,
    filter_no_terminal_punct: bool,
    min_num_sentences: usize,
    min_words_per_line: usize, // Changed to per line to match Python default
    max_word_length: usize,
    filter_lorem_ipsum: bool,
    filter_javascript: bool,
    filter_curly_bracket: bool,
    filter_policy: bool,
    // language: String, // Language handling for sentence splitting might be more complex in Rust
}

impl C4QualityFilter {
    /// Create a new C4QualityFilter.
    ///
    /// # Arguments
    /// * `split_paragraph` - whether to split by paragraph (\n) or sentence (not fully implemented yet)
    /// * `remove_citations` - remove wikipedia style citations
    /// * `filter_no_terminal_punct` - remove lines without terminal punctuation
    /// * `min_num_sentences` - minimum total sentences required in the document (after line filtering)
    /// * `min_words_per_line` - minimum words each line must contain
    /// * `max_word_length` - maximum allowed length for any word in a line
    /// * `filter_lorem_ipsum` - filter documents containing "lorem ipsum"
    /// * `filter_javascript` - filter lines containing "javascript"
    /// * `filter_curly_bracket` - filter documents containing "{"
    /// * `filter_policy` - filter lines containing policy substrings
    pub fn new(
        split_paragraph: bool,
        remove_citations: bool,
        filter_no_terminal_punct: bool,
        min_num_sentences: usize,
        min_words_per_line: usize,
        max_word_length: usize,
        filter_lorem_ipsum: bool,
        filter_javascript: bool,
        filter_curly_bracket: bool,
        filter_policy: bool,
        // language: String,
    ) -> Self {
        C4QualityFilter {
            split_paragraph,
            remove_citations,
            filter_no_terminal_punct,
            min_num_sentences,
            min_words_per_line,
            max_word_length,
            filter_lorem_ipsum,
            filter_javascript,
            filter_curly_bracket,
            filter_policy,
            // language,
        }
    }
}

#[async_trait]
impl ProcessingStep for C4QualityFilter {
    fn name(&self) -> &'static str {
        "C4QualityFilter"
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        let mut document = document;
        let original_content = document.content.clone();
        let lines: Vec<&str> = if self.split_paragraph {
            document.content.lines().collect()
        } else {
            // Sentence splitting logic would go here if implemented
            // For now, we'll stick to line splitting as the primary mode
            split_into_sentences(&document.content) // Default to lines for now
        };

        let mut kept_lines: Vec<String> = Vec::new();
        let mut filter_reasons: Vec<String> = Vec::new();

        // Document-level checks that can fail early
        if self.filter_lorem_ipsum && original_content.to_lowercase().contains("lorem ipsum") {
            filter_reasons.push("lorem_ipsum".to_string());
        }
        if self.filter_curly_bracket && original_content.contains('{') {
            filter_reasons.push("curly_bracket".to_string());
        }

        if !filter_reasons.is_empty() {
            let reasons_string = filter_reasons.join("; ");
            document
                .metadata
                .insert("c4_filter_status".to_string(), "filtered".to_string());
            document
                .metadata
                .insert("c4_filter_reasons".to_string(), reasons_string.clone());
            return Err(PipelineError::DocumentFiltered {
                document,
                reason: reasons_string,
            });
        }

        // Line-by-line filtering
        for line in lines {
            let current_line = line.trim().to_string();

            let processed_line = if self.remove_citations {
                CITATION_REGEX.replace_all(&current_line, "").to_string()
            } else {
                current_line
            };

            {
                // Start a new scope for borrows
                let line_l = processed_line.to_lowercase();
                let words = split_into_words(&processed_line);

                // max_word_length filter
                if self.max_word_length > 0
                    && words
                        .iter()
                        .any(|w| w.chars().count() > self.max_word_length)
                {
                    continue; // Drop line
                }

                // end punctuation
                if self.filter_no_terminal_punct {
                    let ends_with_terminal_punct = processed_line
                        .chars()
                        .last()
                        .is_some_and(|last_char| END_PUNCTUATION.contains(&last_char));
                    let ends_with_ellipsis = processed_line.ends_with(ELLIPSIS);

                    if !ends_with_terminal_punct || ends_with_ellipsis {
                        continue; // Drop line
                    }
                }

                // min words per line
                if self.min_words_per_line > 0 && words.len() < self.min_words_per_line {
                    continue; // Drop line
                }

                // javascript filter
                if self.filter_javascript && line_l.contains("javascript") {
                    continue; // Drop line
                }

                // policy filter
                if self.filter_policy && POLICY_SUBSTRINGS.iter().any(|p| line_l.contains(p)) {
                    continue; // Drop line
                }
            } // End of scope, line_l and words are dropped

            // If line passes all filters, keep it
            kept_lines.push(processed_line);
        }

        // Reconstruct document content from kept lines
        document.content = kept_lines.join("\n").trim().to_string();

        // Sentence count check on the filtered content
        let sentences_in_kept_content = split_into_sentences(&document.content);
        let actual_sentence_count = sentences_in_kept_content.len();

        if self.min_num_sentences > 0 && actual_sentence_count < self.min_num_sentences {
            filter_reasons.push(format!(
                "too_few_sentences (found {}, required {})",
                actual_sentence_count, self.min_num_sentences
            ));
        }

        // Final Decision
        if !filter_reasons.is_empty() {
            let reasons_string = filter_reasons.join("; ");
            document
                .metadata
                .insert("c4_filter_status".to_string(), "filtered".to_string());
            document
                .metadata
                .insert("c4_filter_reasons".to_string(), reasons_string.clone());
            Err(PipelineError::DocumentFiltered {
                document,
                reason: reasons_string,
            })
        } else {
            document
                .metadata
                .insert("c4_filter_status".to_string(), "passed".to_string());
            Ok(document)
        }
    }
}

pub struct C4BadWordsFilter {
    params: C4BadWordsParams,
    badwords_regex_map: HashMap<String, Regex>,
    rng: StdRng,
}

impl C4BadWordsFilter {
    pub fn new(params: C4BadWordsParams) -> Self {
        let rng = match params.seed {
            Some(seed_val) => StdRng::seed_from_u64(seed_val),
            None => StdRng::from_entropy(), // Or StdRng::from_rng(thread_rng()).unwrap() for a different approach
        };
        C4BadWordsFilter {
            params,
            badwords_regex_map: HashMap::new(),
            rng,
        }
    }

    // Helper method to get or compile badwords regex for a language
    fn _get_badwords(&mut self, lang: &str) -> Result<Option<&Regex>> {
        if self.badwords_regex_map.contains_key(lang) {
            return Ok(self.badwords_regex_map.get(lang));
        }

        // Simulate checking for badwords list availability
        // TODO: Implement actual badwords list loading and regex compilation.
        // For now, we'll assume lists are not available for any language other than a test 'xx'.
        let badwords_available = lang == "xx";

        if !badwords_available {
            if self.params.fail_on_missing_language {
                // Constructing an error that includes the document ID and language.
                // Since we don't have the document ID here, we'll return a more generic error.
                // The `process` method will have to handle wrapping this into a DocumentFiltered error.
                return Err(PipelineError::FilterError {
                    filter_name: "C4BadWordsFilter".to_string(),
                    reason: format!(
                        "There is no badwords list available for '{}'. Set fail_on_missing_language=False to continue anyway.",
                        lang
                    ),
                });
            } else {
                // If not failing, we mark this language as having no regex (None)
                // but store it to avoid re-processing, effectively skipping the check next time for this lang.
                // However, since we return Option<&Regex>, we can't store None directly in badwords_regex_map.
                // So, we just return Ok(None) and don't populate the map for this case.
                return Ok(None);
            }
        }

        // Placeholder for actual badwords loading and regex compilation
        // For example purposes, let's compile a dummy regex if lang is "xx"
        if lang == "xx" {
            // Simulate loading badwords and compiling regex
            // let badwords: Vec<String> = vec!["dummybadword".to_string()]; // Example
            // let pattern = format!(r"(?i)(?:\W|^)({})(?:\W|$)", badwords.join("|")); // Case-insensitive
            let pattern = r"(?i)(?:\W|^)(dummybadword)(?:\W|$)"; // Simple dummy
            match Regex::new(&pattern) {
                Ok(re) => {
                    self.badwords_regex_map.insert(lang.to_string(), re);
                    return Ok(self.badwords_regex_map.get(lang));
                }
                Err(e) => {
                    return Err(PipelineError::FilterError {
                        filter_name: "C4BadWordsFilter".to_string(),
                        reason: format!("Failed to compile regex for lang '{}': {}", lang, e),
                    });
                }
            }
        }

        // Should not be reached if badwords_available is true and lang is "xx"
        // or if badwords_available is false.
        // This path implies a language for which badwords_available was true, but no specific logic handled it.
        // For robust implementation, this should be an error or specific handling.
        // Given the current placeholder, this means we are not failing but also don't have words for a supposedly available list.
        Ok(None)
    }
}

#[async_trait]
impl ProcessingStep for C4BadWordsFilter {
    fn name(&self) -> &'static str {
        "C4BadWordsFilter"
    }

    async fn process(&mut self, mut document: TextDocument) -> Result<TextDocument> {
        let lang = document
            .metadata
            .get("language")
            .map(|s| s.as_str())
            .unwrap_or_else(|| self.params.default_language.as_str());

        let badwords_regex_option = match self._get_badwords(lang) {
            Ok(re_opt) => re_opt,
            Err(e) => {
                // If _get_badwords returns an error (e.g. fail_on_missing_language=true)
                // We need to convert it to a DocumentFiltered error.
                // The original error reason from _get_badwords can be used.
                let reason = match e {
                    PipelineError::FilterError { reason, .. } => reason,
                    _ => format!("Failed to get badwords for language '{}'", lang),
                };
                document.metadata.insert(
                    "c4_badwords_filter_status".to_string(),
                    "filtered".to_string(),
                );
                document.metadata.insert(
                    "c4_badwords_filter_reason".to_string(),
                    reason.clone(),
                );
                return Err(PipelineError::DocumentFiltered { document, reason });
            }
        };

        if badwords_regex_option.is_none() {
            // No regex available, but fail_on_missing_language was false.
            // Or, the language was 'xx' but something went wrong in placeholder logic.
            // self.stat_update("missing_badwords_lang", f"missing_badwords_lang_{lang}"); // Placeholder
            document.metadata.insert(
                "c4_badwords_filter_status".to_string(),
                "passed_no_regex".to_string(), // Or "passed_missing_lang_list"
            );
            return Ok(document);
        }

        let badwords_regex = badwords_regex_option.unwrap(); // Safe due to check above

        // Search for bad words (case-insensitive already handled by regex pattern in _get_badwords if (?i) is used)
        // The Python code uses doc.text.lower(), but Rust regex can be case-insensitive.
        // Assuming the regex compiled in _get_badwords will handle case insensitivity.
        if badwords_regex.is_match(&document.content) {
            // self.stat_update("documents_with_badwords", f"documents_with_badwords_{lang}"); // Placeholder

            if self.params.keep_fraction > 0.0 && self.rng.gen::<f32>() < self.params.keep_fraction {
                // self.stat_update("document_kept_with_badwords", f"document_kept_with_badwords_{lang}"); // Placeholder
                document.metadata.insert(
                    "c4_badwords_filter_status".to_string(),
                    "passed_kept_by_fraction".to_string(),
                );
                Ok(document)
            } else {
                // self.stat_update(f"document_removed_with_badwords_{lang}"); // Placeholder
                let reason = "document_removed_with_badwords".to_string();
                document.metadata.insert(
                    "c4_badwords_filter_status".to_string(),
                    "filtered".to_string(),
                );
                document.metadata.insert(
                    "c4_badwords_filter_reason".to_string(),
                    reason.clone(),
                );
                Err(PipelineError::DocumentFiltered { document, reason })
            }
        } else {
            // No bad words found
            document.metadata.insert(
                "c4_badwords_filter_status".to_string(),
                "passed".to_string(),
            );
            Ok(document)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::C4BadWordsParams; // Added
    use crate::data_model::TextDocument;
    use crate::error::PipelineError; // Added
    use std::collections::HashMap;

    // Helper to create a TextDocument for tests
    fn create_test_doc(id: &str, content: &str) -> TextDocument {
        TextDocument {
            id: id.to_string(),
            source: "test_source".to_string(),
            content: content.to_string(),
            metadata: HashMap::new(),
        }
    }

    // Helper to create a filter with common settings
    fn default_filter() -> C4QualityFilter {
        C4QualityFilter::new(
            true, // split_paragraph
            true, // remove_citations
            true, // filter_no_terminal_punct
            5,    // min_num_sentences
            3,    // min_words_per_line
            1000, // max_word_length
            true, // filter_lorem_ipsum
            true, // filter_javascript
            true, // filter_curly_bracket
            true, // filter_policy
        )
    }

    #[tokio::test]
    async fn test_document_passes() {
        let filter = default_filter();
        let doc_content = "This is the first sentence. This is the second sentence. This is the third sentence. This is the fourth sentence. This is the fifth sentence.";
        let doc = create_test_doc("pass1", doc_content);
        let result = filter.process(doc).await;
        assert!(result.is_ok(), "Document should pass: {:?}", result.err());
        let processed_doc = result.unwrap();
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
        // Check content is unchanged if no line filters applied
        assert_eq!(processed_doc.content.trim(), doc_content.trim());
    }

    #[tokio::test]
    async fn test_too_few_sentences() {
        let filter = default_filter(); // min_num_sentences = 5
        let doc_content = "One sentence. Two sentences. Three sentences. Four sentences.";
        let doc = create_test_doc("fail_sentences", doc_content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
            assert!(reason.contains("too_few_sentences (found 4, required 5)"));
        } else {
            panic!("Expected DocumentFiltered error, got different error or Ok");
        }
    }

    #[tokio::test]
    async fn test_line_too_few_words() {
        let filter = default_filter(); // min_words_per_line = 3
        let doc_content = "This line is fine.\nTwo words.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.";
        let doc = create_test_doc("fail_line_words", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after dropping line: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // The line "Two words." should be dropped
        assert_eq!(processed_doc.content.trim(), "This line is fine.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_line_missing_terminal_punctuation() {
        let filter = default_filter(); // filter_no_terminal_punct = true
        let doc_content = "This line is fine.\nThis one is not\nAnd this is okay. Here is another sentence. And a fifth one. This is the sixth sentence.";
        let doc = create_test_doc("fail_line_punc", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after dropping line: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // The line "This one is not" should be dropped
        assert_eq!(processed_doc.content.trim(), "This line is fine.\nAnd this is okay. Here is another sentence. And a fifth one. This is the sixth sentence.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_line_ends_with_ellipsis() {
        let filter = default_filter(); // filter_no_terminal_punct = true
        let doc_content = "This line is fine.\nThis one ends with ellipsis...\nAnd this is okay. This is the fourth sentence. And the fifth sentence. Here is the sixth.";
        let doc = create_test_doc("fail_line_ellipsis", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after dropping line: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // The line "This one ends with ellipsis..." should be dropped
        assert_eq!(processed_doc.content.trim(), "This line is fine.\nAnd this is okay. This is the fourth sentence. And the fifth sentence. Here is the sixth.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_word_too_long() {
        let filter = default_filter(); // max_word_length = 1000
        let long_word = "a".repeat(1001);
        let doc_content = format!("This line is fine.\nA line with a verylongword {}.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.", long_word);
        let doc = create_test_doc("fail_word_length", &doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after dropping line: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // The line with the long word should be dropped
        assert_eq!(processed_doc.content.trim(), "This line is fine.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_filter_lorem_ipsum() {
        let filter = default_filter(); // filter_lorem_ipsum = true
        let doc_content = "This is fine. Lorem ipsum dolor sit amet. This is also fine.";
        let doc = create_test_doc("fail_lorem_ipsum", doc_content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
            assert!(reason.contains("lorem_ipsum"));
        } else {
            panic!("Expected DocumentFiltered error");
        }
    }

    #[tokio::test]
    async fn test_filter_javascript() {
        let filter = default_filter(); // filter_javascript = true
        let doc_content = "This is fine.\nSome javascript code here.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.";
        let doc = create_test_doc("fail_javascript", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after dropping line: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // The line with "javascript" should be dropped
        assert_eq!(processed_doc.content.trim(), "This is fine.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_filter_curly_bracket() {
        let filter = default_filter(); // filter_curly_bracket = true
        let doc_content = "This is fine.\nSome code block {}.\nAnother good line.";
        let doc = create_test_doc("fail_curly_bracket", doc_content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
            assert!(reason.contains("curly_bracket"));
        } else {
            panic!("Expected DocumentFiltered error");
        }
    }

    #[tokio::test]
    async fn test_filter_policy() {
        let filter = default_filter(); // filter_policy = true
        let doc_content = "This is fine.\nRead our privacy policy.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.";
        let doc = create_test_doc("fail_policy", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after dropping line: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // The line with "privacy policy" should be dropped
        assert_eq!(processed_doc.content.trim(), "This is fine.\nAnother good line. This is the fourth sentence. And the fifth sentence. Here is the sixth.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_remove_citations() {
        let filter = default_filter(); // remove_citations = true
        let doc_content = "This is text [1]. Another sentence [2, 3]. Final text [45]. Here is the fourth sentence. And the fifth sentence. This is the sixth sentence.";
        let doc = create_test_doc("remove_citations", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass after removing citations: {:?}",
            result.err()
        );
        let processed_doc = result.unwrap();
        // Citations should be removed
        assert_eq!(processed_doc.content.trim(), "This is text . Another sentence . Final text . Here is the fourth sentence. And the fifth sentence. This is the sixth sentence.");
        assert_eq!(
            processed_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
    }

    #[tokio::test]
    async fn test_empty_document_content() {
        let filter = default_filter(); // min_num_sentences = 5
        let doc = create_test_doc("empty_content", "");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
            assert!(reason.contains("too_few_sentences (found 0, required 5)"));
        } else {
            panic!("Expected DocumentFiltered error for empty content");
        }
    }

    #[tokio::test]
    async fn test_content_just_spaces() {
        let filter = default_filter(); // min_num_sentences = 5
        let doc = create_test_doc("space_content", "   \n   ");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
            // After trimming and filtering empty lines, there are 0 sentences.
            assert!(reason.contains("too_few_sentences (found 0, required 5)"));
        } else {
            panic!("Expected DocumentFiltered error for space content");
        }
    }

    #[tokio::test]
    async fn test_zero_min_values_pass_minimal_doc() {
        let filter = C4QualityFilter::new(
            true,  // split_paragraph
            false, // remove_citations
            false, // filter_no_terminal_punct
            0,     // min_num_sentences
            0,     // min_words_per_line
            0,     // max_word_length (0 means no limit in this context)
            false, // filter_lorem_ipsum
            false, // filter_javascript
            false, // filter_curly_bracket
            false, // filter_policy
        );
        let doc = create_test_doc("zero_min_pass", "Ok."); // 1 sentence, 1 word, word length 2.
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass with zero min values: {:?}",
            result.err()
        );
    }
}

// Helper to create C4BadWordsParams for tests
fn badwords_params(
    keep_fraction: f32,
    fail_on_missing_language: bool,
    seed: Option<u64>,
    default_language: &str,
) -> C4BadWordsParams {
    C4BadWordsParams {
        keep_fraction,
        fail_on_missing_language,
        seed,
        default_language: default_language.to_string(),
    }
}

#[tokio::test]
async fn test_badwords_document_passes_no_badwords() {
    let params = badwords_params(0.0, true, Some(123), "en");
    let mut filter = C4BadWordsFilter::new(params);
    // Using lang "xx" which has "dummybadword" as a bad word in placeholder _get_badwords.
    // This text does not contain "dummybadword".
    let doc = create_test_doc("bw_pass_nobadwords", "This is a clean sentence.");
    let mut doc_with_lang = doc.clone();
    doc_with_lang.metadata.insert("language".to_string(), "xx".to_string());

    let result = filter.process(doc_with_lang).await;
    assert!(result.is_ok(), "Document should pass: {:?}", result.err());
    let processed_doc = result.unwrap();
    assert_eq!(
        processed_doc.metadata.get("c4_badwords_filter_status"),
        Some(&"passed".to_string())
    );
}

#[tokio::test]
async fn test_badwords_document_filtered_has_badwords() {
    let params = badwords_params(0.0, true, Some(123), "en");
    let mut filter = C4BadWordsFilter::new(params);
    // Using lang "xx" which has "dummybadword" as a bad word.
    let doc = create_test_doc("bw_filter_hasbadwords", "This sentence contains a dummybadword here.");
    let mut doc_with_lang = doc.clone();
    doc_with_lang.metadata.insert("language".to_string(), "xx".to_string());

    let result = filter.process(doc_with_lang).await;
    assert!(result.is_err(), "Document should be filtered");
    if let Err(PipelineError::DocumentFiltered { reason, document: filtered_doc }) = result {
        assert_eq!(reason, "document_removed_with_badwords");
        assert_eq!(
            filtered_doc.metadata.get("c4_badwords_filter_status"),
            Some(&"filtered".to_string())
        );
    } else {
        panic!("Expected DocumentFiltered error, got different error or Ok. Result: {:?}", result);
    }
}

#[tokio::test]
async fn test_badwords_keep_fraction_keeps_doc() {
    // Test that with keep_fraction = 1.0, the document is kept even if it has badwords
    let params = badwords_params(1.0, true, Some(123), "en"); // keep_fraction = 1.0
    let mut filter = C4BadWordsFilter::new(params);
    let doc = create_test_doc("bw_keep_fraction", "Another dummybadword sentence.");
    let mut doc_with_lang = doc.clone();
    doc_with_lang.metadata.insert("language".to_string(), "xx".to_string());

    let result = filter.process(doc_with_lang).await;
    assert!(result.is_ok(), "Document should be kept by keep_fraction: {:?}", result.err());
    let processed_doc = result.unwrap();
    assert_eq!(
        processed_doc.metadata.get("c4_badwords_filter_status"),
        Some(&"passed_kept_by_fraction".to_string())
    );
}

#[tokio::test]
async fn test_badwords_keep_fraction_filters_doc() {
    // Test that with keep_fraction = 0.0, the document is filtered if bad words are present.
    // Any float from rng.gen::<f32>() is >= 0.0.
    // So, if keep_fraction is 0.0, (rng.gen::<f32>() < params.keep_fraction) will always be false.
    let params = badwords_params(0.0, true, Some(123), "en"); // keep_fraction = 0.0
    let mut filter = C4BadWordsFilter::new(params);
    let doc = create_test_doc("bw_filter_fraction_zero", "A sentence with dummybadword.");
    let mut doc_with_lang = doc.clone();
    doc_with_lang.metadata.insert("language".to_string(), "xx".to_string());

    let result = filter.process(doc_with_lang).await;
    assert!(result.is_err(), "Document should be filtered with keep_fraction=0.0");
     if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
        assert_eq!(reason, "document_removed_with_badwords");
    } else {
        panic!("Expected DocumentFiltered error. Result: {:?}", result);
    }
}


#[tokio::test]
async fn test_badwords_missing_language_fail() {
    let params = badwords_params(0.0, true, Some(123), "en"); // fail_on_missing_language = true
    let mut filter = C4BadWordsFilter::new(params);
    // Using "zz" for which no badword list is set up in the placeholder _get_badwords
    let doc = create_test_doc("bw_missing_lang_fail", "Some text.");
    let mut doc_with_lang = doc.clone();
    doc_with_lang.metadata.insert("language".to_string(), "zz".to_string());

    let result = filter.process(doc_with_lang).await;
    assert!(result.is_err(), "Should fail due to missing language and fail_on_missing_language=true");
    if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
        assert!(reason.contains("There is no badwords list available for 'zz'"), "Unexpected reason: {}", reason);
    } else {
        panic!("Expected DocumentFiltered error for missing language. Result: {:?}", result);
    }
}

#[tokio::test]
async fn test_badwords_missing_language_pass() {
    let params = badwords_params(0.0, false, Some(123), "en"); // fail_on_missing_language = false
    let mut filter = C4BadWordsFilter::new(params);
    let doc = create_test_doc("bw_missing_lang_pass", "Some text.");
    let mut doc_with_lang = doc.clone();
    doc_with_lang.metadata.insert("language".to_string(), "zz".to_string()); // "zz" has no list

    let result = filter.process(doc_with_lang).await;
    assert!(result.is_ok(), "Should pass due to fail_on_missing_language=false: {:?}", result.err());
    let processed_doc = result.unwrap();
    assert_eq!(
        processed_doc.metadata.get("c4_badwords_filter_status"),
        Some(&"passed_no_regex".to_string())
    );
}

#[tokio::test]
async fn test_badwords_default_language_used() {
    let params = badwords_params(0.0, true, Some(123), "xx"); // default_language = "xx"
    let mut filter = C4BadWordsFilter::new(params);
    // Document has no "language" metadata, so "xx" (default) should be used.
    // "xx" has "dummybadword" as a bad word.
    let doc = create_test_doc("bw_default_lang", "Text with dummybadword.");

    let result = filter.process(doc).await; // No explicit language set on doc
    assert!(result.is_err(), "Should be filtered based on default language 'xx'");
    if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
        assert_eq!(reason, "document_removed_with_badwords");
    } else {
        panic!("Expected DocumentFiltered error due to default language processing. Result: {:?}", result);
    }
}

#[tokio::test]
async fn test_badwords_default_language_clean() {
    let params = badwords_params(0.0, true, Some(123), "xx"); // default_language = "xx"
    let mut filter = C4BadWordsFilter::new(params);
    // Document has no "language" metadata, so "xx" (default) should be used.
    // This text is clean for lang "xx".
    let doc = create_test_doc("bw_default_lang_clean", "Clean text for default lang.");

    let result = filter.process(doc).await; // No explicit language set on doc
    assert!(result.is_ok(), "Should pass with default language 'xx': {:?}", result.err());
     let processed_doc = result.unwrap();
    assert_eq!(
        processed_doc.metadata.get("c4_badwords_filter_status"),
        Some(&"passed".to_string())
    );
}
