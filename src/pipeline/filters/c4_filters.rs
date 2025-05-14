use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::executor::ProcessingStep;
use crate::utils::text::{split_into_sentences, split_into_words};

use async_trait::async_trait;
use lazy_static::lazy_static;
use regex::Regex;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::TextDocument;
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
