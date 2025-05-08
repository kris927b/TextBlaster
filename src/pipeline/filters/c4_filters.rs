// src/pipeline/filters/C4_filter.rs (or wherever your filter step is)

use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::executor::ProcessingStep;
use crate::utils::text::{split_into_sentences, split_into_words};

use async_trait::async_trait;

/// A composite filter that enforces multiple quality checks on documents:
/// 1) Minimum number of sentences
/// 2) Minimum number of words per sentence
/// 3) Maximum word length
/// 4) Sentence terminal punctuation
pub struct C4QualityFilter {
    min_sentences: usize,
    min_words_per_sentence: usize,
    max_word_length: usize,
}

impl C4QualityFilter {
    /// Create a new C4QualityFilter.
    ///
    /// # Arguments
    /// * `min_sentences` - the minimum total sentences required
    /// * `min_words_per_sentence` - the minimum words each sentence must contain
    /// * `max_word_length` - the maximum allowed length for any word
    pub fn new(
        min_sentences: usize,
        min_words_per_sentence: usize,
        max_word_length: usize,
    ) -> Self {
        C4QualityFilter {
            min_sentences,
            min_words_per_sentence,
            max_word_length,
        }
    }
}

#[async_trait]
impl ProcessingStep for C4QualityFilter {
    fn name(&self) -> &'static str {
        "C4QualityFilter"
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        // Split into sentences
        let sentences = split_into_sentences(&document.content);
        let sentence_count = sentences.len();
        if sentence_count < self.min_sentences {
            let reason = format!(
                "Too few sentences (found {}, required {})",
                sentence_count, self.min_sentences
            );
            return Err(PipelineError::DocumentFiltered {
                doc_id: document.id.clone(),
                reason,
            });
        }

        // Check each sentence for word count and terminal punctuation
        for sentence in &sentences {
            let trimmed_sentence = sentence.trim(); // Use a different variable name
            if trimmed_sentence.is_empty() {
                // This can happen if split_into_sentences returns empty strings or strings with only whitespace
                // If min_sentences is 0 or 1, and content is "   ", sentences might be ["   "] or [].
                // If sentences = [" "], trimmed_sentence is "", word_count is 0.
                // If min_words_per_sentence > 0, this empty sentence would be filtered.
                // This seems fine. If a "sentence" is just whitespace, it shouldn't count towards content.
                // Consider if a document full of " . . . " should pass sentence count but fail word count.
                continue;
            }

            // Words per sentence
            let words = split_into_words(trimmed_sentence);
            let word_count = words.len();
            if word_count < self.min_words_per_sentence {
                let reason = format!(
                    "Too few words in sentence: \"{}\" (found {}, required {})", // Added sentence for context
                    trimmed_sentence, word_count, self.min_words_per_sentence
                );
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason,
                });
            }

            // Terminal punctuation
            if let Some(last_char) = trimmed_sentence.chars().last() {
                let valid_end = matches!(last_char, '.' | '!' | '?' | '"' | '\'');
                if !valid_end {
                    let reason = format!(
                        "Sentence does not end with valid punctuation (sentence: \"{}\", last_char: '{}')",
                        trimmed_sentence, last_char
                    );
                    return Err(PipelineError::DocumentFiltered {
                        doc_id: document.id.clone(),
                        reason,
                    });
                }
            } else {
                // This case means trimmed_sentence is empty, but we already `continue`d above if it was.
                // So, this `else` block for `if let Some(last_char)` should not be reachable if trimmed_sentence is not empty.
                // If min_words_per_sentence is 0, an empty sentence could reach here.
                // If an "empty" sentence (e.g. from "Text. . Text") is considered a sentence by the splitter,
                // and min_words_per_sentence is 0, it could lack a last char.
                // However, `split_into_words("")` is `[]`, so word_count would be 0.
                // If min_words_per_sentence > 0, it would have been filtered already.
                // If min_words_per_sentence == 0, an empty sentence "passes" word count.
                // Does an empty sentence need terminal punctuation? The rule implies non-empty.
                // Let's assume sentences must be non-empty to require punctuation.
                // The `trimmed_sentence.is_empty()` check at the start of the loop handles this.
            }
        }

        // Check maximum word length across all content
        // This can be slow if document.content is huge and already split for sentences.
        // Consider if this check should be done per sentence word, or globally.
        // Current implementation is global.
        let all_words = split_into_words(&document.content);
        for word in all_words {
            if word.chars().count() > self.max_word_length {
                let reason = format!(
                    "Word exceeds max length (word: \"{}\", length: {}, max: {})",
                    word,
                    word.chars().count(), // Use chars().count() for length consistency
                    self.max_word_length
                );
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason,
                });
            }
        }

        // If all checks pass, return the document for next step
        Ok(document)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::TextDocument;
    use std::collections::HashMap; // For TextDocument metadata

    // Helper to create a TextDocument for tests
    fn create_test_doc(id: &str, content: &str) -> TextDocument {
        TextDocument {
            id: id.to_string(),
            source: "test_source".to_string(),
            content: content.to_string(),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_document_passes() {
        let filter = C4QualityFilter::new(3, 3, 10);
        let doc_content = "Hello world, this is a test. It should pass the filter! Is this okay?";
        let doc = create_test_doc("pass1", doc_content);
        let result = filter.process(doc.clone()).await;
        assert!(result.is_ok(), "Document should pass: {:?}", result.err());
        if let Ok(processed_doc) = result {
            assert_eq!(processed_doc.id, doc.id); // Ensure the same doc is returned
        }
    }

    #[tokio::test]
    async fn test_too_few_sentences() {
        let filter = C4QualityFilter::new(2, 1, 10);
        let doc = create_test_doc("fail_sentences", "Hello world."); // 1 sentence
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("Too few sentences (found 1, required 2)"));
            }
            _ => panic!("Expected DocumentFiltered error"),
        }
    }

    #[tokio::test]
    async fn test_too_few_words_in_sentence() {
        let filter = C4QualityFilter::new(2, 3, 10);
        let doc = create_test_doc("fail_words", "First sentence is fine. Oops."); // "Oops." has 1 word
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.contains("Too few words in sentence: \"Oops.\" (found 1, required 3)")
                );
            }
            _ => panic!("Expected DocumentFiltered error"),
        }
    }

    #[tokio::test]
    async fn test_sentence_ends_with_quote_passes() {
        let filter = C4QualityFilter::new(1, 2, 10);
        let doc_content = "He said \"Hello world\"."; // Ends with quote, then period.
        let doc = create_test_doc("pass_quote_end", doc_content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document ending with quote then period should pass: {:?}",
            result.err()
        );

        let doc_content_2 = "She asked 'Why not?'"; // Ends with quote, then question mark.
        let doc2 = create_test_doc("pass_quote_end_2", doc_content_2);
        let result2 = filter.process(doc2).await;
        assert!(
            result2.is_ok(),
            "Document ending with quote then QM should pass: {:?}",
            result2.err()
        );
    }

    #[tokio::test]
    async fn test_missing_terminal_punctuation() {
        let filter = C4QualityFilter::new(2, 2, 10);
        let doc = create_test_doc("fail_punctuation", "This is okay. This is not"); // "This is not" lacks terminal punc
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains("Sentence does not end with valid punctuation (sentence: \"This is not\", last_char: 't')"));
            }
            _ => panic!("Expected DocumentFiltered error"),
        }
    }

    #[tokio::test]
    async fn test_word_too_long() {
        let filter = C4QualityFilter::new(1, 2, 10);
        // "shortsentence" is 13 chars long.
        let doc = create_test_doc("fail_word_length", "A shortsentence here.");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.contains(
                    "Word exceeds max length (word: \"shortsentence\", length: 13, max: 10)"
                ));
            }
            _ => panic!("Expected DocumentFiltered error"),
        }
    }

    #[tokio::test]
    async fn test_empty_document_content() {
        let filter = C4QualityFilter::new(1, 1, 10);
        let doc = create_test_doc("empty_content", "");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // "" -> 0 sentences.
                assert!(reason.contains("Too few sentences (found 0, required 1)"));
            }
            _ => panic!("Expected DocumentFiltered error for empty content"),
        }
    }

    #[tokio::test]
    async fn test_content_just_spaces() {
        let filter = C4QualityFilter::new(1, 1, 10);
        let doc = create_test_doc("space_content", "   ");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // "   " -> 0 sentences after trimming in split_into_sentences.
                assert!(reason.contains("Too few sentences (found 0, required 1)"));
            }
            _ => panic!("Expected DocumentFiltered error for space content"),
        }
    }

    #[tokio::test]
    async fn test_zero_min_values_pass_minimal_doc() {
        let filter = C4QualityFilter::new(0, 0, 5);
        let doc = create_test_doc("zero_min_pass", "Ok."); // 1 sentence, 1 word, word length 2.
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document should pass with zero min values: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_zero_min_words_sentence_pass_empty_sentence() {
        // This tests if a sentence like " ." (which becomes "" after trimming, 0 words) passes if min_words_per_sentence is 0.
        // And if it needs punctuation. Current logic skips empty trimmed sentences for punctuation check.
        let filter = C4QualityFilter::new(1, 0, 5); // 1 sentence, 0 words per sentence minimum
        let doc = create_test_doc("zero_words_empty_sentence", " . ");
        // `split_into_sentences(" . ")` might give `["."]` (depends on segmenter, after our trim)
        // If `sentences` is `["."]`, then `trimmed_sentence` is `"."`. `split_into_words(".")` is `[]`. word_count is 0. Passes.
        // `last_char` is `.`. Passes.
        let result = filter.process(doc).await;
        assert!(result.is_ok(), "Document with effectively empty but punctuated sentence should pass if min_words=0: {:?}", result.err());

        // Test with no punctuation on the empty sentence
        // let filter_no_punc = C4QualityFilter::new(1, 0, 5);
        // let doc_no_punc = create_test_doc("zero_words_empty_sentence_no_punc", "Hello world");
        // The above test doc will fail because "Hello world" (as a sentence) does not end with punctuation.
        // The filter expects valid punctuation if the sentence is non-empty after trimming.
    }

    #[tokio::test]
    async fn test_sentence_with_only_punctuation() {
        // Part 1: Test that a document can pass if an "empty" sentence (like ".")
        // is allowed due to min_words_per_sentence = 0.
        let filter_part1 = C4QualityFilter::new(1, 0, 5); // max_word_length = 5
        let doc_part1 = create_test_doc("punc_only_sentence_part1", "This is text. . Next one.");
        let result_part1 = filter_part1.process(doc_part1).await;
        assert!(result_part1.is_ok(), "Part 1: Should pass with sentence '.' if min_words=0. Actual: {:?}", result_part1.err());

        // Part 2: Test that a sentence consisting only of punctuation (like ".")
        // FAILS if min_words_per_sentence > 0.
        let filter_part2 = C4QualityFilter::new(1, 1, 5); // min_sentences = 1, min_words_per_sentence = 1, max_word_length = 5
        
        // Test with a multi-sentence document where one sentence is just "."
        // Using an even simpler string that should isolate the "." sentence failure.
        let doc_part2_multi = create_test_doc("punc_only_sentence_fail_multi", ". .");
        let result_part2_multi = filter_part2.process(doc_part2_multi.clone()).await;
        
        // Expected: The first "." sentence should fail because word_count (0) < min_words_per_sentence (1).
        assert!(result_part2_multi.is_err(), "Part 2 (multi-sentence '. .'): Document should fail. Actual: {:?}", result_part2_multi.as_ref().map(|_| "Ok").unwrap_or("Err"));
        if let Err(PipelineError::DocumentFiltered { ref reason, .. }) = result_part2_multi {
            // Adjust expectation: split_into_sentences likely treats ". ." as a single sentence ". .".
            assert!(reason.contains("Too few words in sentence: \". .\" (found 0, required 1)"), "Incorrect reason for multi-sentence '. .': {}", reason);
        } else if result_part2_multi.is_ok() {
            panic!("Part 2 (multi-sentence '. .') expected Err, got Ok. Doc: {:?}", doc_part2_multi);
        } else {
            panic!("Part 2 (multi-sentence '. .') expected DocumentFiltered error, got different error: {:?}", result_part2_multi.err());
        }


        // Test with a document that is ONLY "."
        let doc_part2_single = create_test_doc("punc_only_sentence_fail_single", ".");
        let result_part2_single = filter_part2.process(doc_part2_single.clone()).await;
        assert!(result_part2_single.is_err(), "Part 2 (single '.'): Document with only '.' sentence should fail if min_words=1. Actual: {:?}", result_part2_single.as_ref().map(|_| "Ok").unwrap_or("Err"));
        if let Err(PipelineError::DocumentFiltered { ref reason, .. }) = result_part2_single {
             assert!(reason.contains("Too few words in sentence: \".\" (found 0, required 1)"), "Incorrect reason for single '.': {}", reason);
        } else if result_part2_single.is_ok() {
            panic!("Part 2 (single '.') expected Err, got Ok. Doc: {:?}", doc_part2_single);
        } else {
            panic!("Part 2 (single '.') expected DocumentFiltered error, got different error: {:?}", result_part2_single.err());
        }
    }
}
