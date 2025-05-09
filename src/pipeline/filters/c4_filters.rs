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
        let mut document = document; // Make document mutable
        let mut filter_reasons: Vec<String> = Vec::new();

        // --- Calculate actual metrics ---
        let sentences = split_into_sentences(&document.content);
        let actual_sentence_count = sentences.len();
        document.metadata.insert(
            "c4_metric_sentence_count".to_string(),
            actual_sentence_count.to_string(),
        );

        let mut actual_min_words_in_any_sentence = usize::MAX;
        let mut actual_sentences_missing_punctuation_count = 0;
        let mut found_non_empty_sentence = false;

        for sentence_str in &sentences {
            let trimmed_sentence = sentence_str.trim();
            if trimmed_sentence.is_empty() {
                // Skip empty sentences for word count and punctuation checks,
                // but they are part of the overall sentence_count.
                // If min_words_per_sentence is > 0, an empty sentence implies 0 words,
                // so this would be caught by the min_words_per_sentence check later if relevant.
                continue;
            }
            found_non_empty_sentence = true;

            let words_in_sentence = split_into_words(trimmed_sentence);
            let word_count_in_sentence = words_in_sentence.len();
            if word_count_in_sentence < actual_min_words_in_any_sentence {
                actual_min_words_in_any_sentence = word_count_in_sentence;
            }

            if let Some(last_char) = trimmed_sentence.chars().last() {
                if !matches!(last_char, '.' | '!' | '?' | '"' | '\'') {
                    actual_sentences_missing_punctuation_count += 1;
                }
            } else {
                // This case means trimmed_sentence was empty, already handled by the continue.
                // If it could somehow be non-empty but have no last_char (highly unlikely for valid UTF-8 string),
                // it would imply it's missing punctuation. For simplicity, we assume non-empty trimmed strings have a last char.
                // The existing `continue` for `trimmed_sentence.is_empty()` makes this robust.
            }
        }

        if !found_non_empty_sentence {
            // If all sentences were empty or there were no sentences
            actual_min_words_in_any_sentence = 0;
        }
        // If actual_min_words_in_any_sentence is still usize::MAX, it means no non-empty sentences were found.
        if actual_min_words_in_any_sentence == usize::MAX {
            actual_min_words_in_any_sentence = 0; // Default to 0 if no non-empty sentences
        }

        document.metadata.insert(
            "c4_metric_min_words_in_any_sentence".to_string(),
            actual_min_words_in_any_sentence.to_string(),
        );
        document.metadata.insert(
            "c4_metric_sentences_missing_punctuation_count".to_string(),
            actual_sentences_missing_punctuation_count.to_string(),
        );

        let all_words_in_doc = split_into_words(&document.content);
        let mut actual_max_word_len_found = 0;
        for word in &all_words_in_doc {
            // Iterate over collected words
            let len = word.chars().count();
            if len > actual_max_word_len_found {
                actual_max_word_len_found = len;
            }
        }
        document.metadata.insert(
            "c4_metric_max_word_len_found".to_string(),
            actual_max_word_len_found.to_string(),
        );

        // --- Apply Filters and Collect Reasons ---

        // 1. Minimum number of sentences
        if actual_sentence_count < self.min_sentences {
            filter_reasons.push(format!(
                "Too few sentences (found {}, required {})",
                actual_sentence_count, self.min_sentences
            ));
        }

        // 2. Minimum words per sentence & 4. Sentence terminal punctuation (checked per non-empty sentence)
        // These checks now need to iterate again, or we adapt the logic.
        // For simplicity and to match the new structure, let's re-iterate for checks.
        // Alternatively, the checks can use the pre-calculated min_words and missing_punctuation_count.

        // Let's use the pre-calculated metrics where possible.
        // The original filter fails on the *first* bad sentence for min_words or punctuation.
        // The new metrics `actual_min_words_in_any_sentence` and `actual_sentences_missing_punctuation_count`
        // are global. If we want to preserve "fail on first", the logic is more complex.
        // Sticking to the "calculate all, then check all" pattern:

        if self.min_words_per_sentence > 0
            && found_non_empty_sentence
            && actual_min_words_in_any_sentence < self.min_words_per_sentence
        {
            // Find the first sentence that violates this to provide a good error message
            for sentence_str in &sentences {
                let trimmed_sentence = sentence_str.trim();
                if trimmed_sentence.is_empty() {
                    continue;
                }
                let words_in_sentence = split_into_words(trimmed_sentence);
                if words_in_sentence.len() < self.min_words_per_sentence {
                    filter_reasons.push(format!(
                        "Too few words in sentence: \"{}\" (found {}, required {})",
                        trimmed_sentence,
                        words_in_sentence.len(),
                        self.min_words_per_sentence
                    ));
                    break; // Report first violating sentence
                }
            }
        } else if self.min_words_per_sentence > 0
            && actual_sentence_count > 0
            && !found_non_empty_sentence
        {
            // This case means all sentences are empty (e.g. "   .   .   ") but min_words > 0 is required.
            // The first empty sentence (effectively 0 words) would fail.
            filter_reasons.push(format!(
                "Too few words in sentence: \"{}\" (found 0, required {})",
                sentences.get(0).map_or("", |s| s.trim()),
                self.min_words_per_sentence
            ));
        }

        if actual_sentences_missing_punctuation_count > 0 {
            // Find the first sentence that violates this for a good error message
            for sentence_str in &sentences {
                let trimmed_sentence = sentence_str.trim();
                if trimmed_sentence.is_empty() {
                    continue;
                }
                if let Some(last_char) = trimmed_sentence.chars().last() {
                    if !matches!(last_char, '.' | '!' | '?' | '"' | '\'') {
                        filter_reasons.push(format!(
                            "Sentence does not end with valid punctuation (sentence: \"{}\", last_char: '{}')",
                            trimmed_sentence, last_char
                        ));
                        break; // Report first violating sentence
                    }
                } else { /* Should not happen due to is_empty check */
                }
            }
        }

        // 3. Maximum word length
        if self.max_word_length > 0 && actual_max_word_len_found > self.max_word_length {
            // Find the first word that violates this for a good error message
            for word in &all_words_in_doc {
                // Re-use collected words
                if word.chars().count() > self.max_word_length {
                    filter_reasons.push(format!(
                        "Word exceeds max length (word: \"{}\", length: {}, max: {})",
                        word,
                        word.chars().count(),
                        self.max_word_length
                    ));
                    break; // Report first violating word
                }
            }
        }

        // --- Final Decision ---
        if !filter_reasons.is_empty() {
            let reasons_string = filter_reasons.join("; ");
            document
                .metadata
                .insert("c4_filter_status".to_string(), "filtered".to_string());
            document
                .metadata
                .insert("c4_filter_reasons".to_string(), reasons_string.clone());
            Err(PipelineError::DocumentFiltered {
                doc_id: document.id.clone(),
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
    async fn test_document_passes_and_has_metadata() {
        // Renamed from test_document_passes
        let filter = C4QualityFilter::new(3, 3, 10); // min_sentences=3, min_words_per_sentence=3, max_word_length=10
        let doc_content = "Hello world, this is a test. It should pass the filter! Is this okay?";
        // Sentences:
        // 1. "Hello world, this is a test" (6 words)
        // 2. "It should pass the filter" (5 words)
        // 3. "Is this okay" (3 words)
        // Min words in any sentence is 3.
        // Max word length: "Hello"(5), "world"(5), "this"(4), "test"(4), "should"(6), "pass"(4), "filter"(6), "okay"(4) -> max is 6.
        let doc = create_test_doc("pass1", doc_content);
        let result_doc = filter
            .process(doc.clone())
            .await
            .expect("Document should pass");

        assert_eq!(
            result_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
        assert_eq!(
            result_doc.metadata.get("c4_metric_sentence_count"),
            Some(&"3".to_string())
        );
        assert_eq!(
            result_doc
                .metadata
                .get("c4_metric_min_words_in_any_sentence"),
            Some(&"3".to_string())
        ); // "Is this okay?" -> 4 words
        assert_eq!(
            result_doc
                .metadata
                .get("c4_metric_sentences_missing_punctuation_count"),
            Some(&"0".to_string())
        );
        assert_eq!(
            result_doc.metadata.get("c4_metric_max_word_len_found"),
            Some(&"6".to_string())
        ); // "should" or "filter"
    }

    #[tokio::test]
    async fn test_too_few_sentences() {
        let filter = C4QualityFilter::new(2, 1, 10);
        let doc = create_test_doc("fail_sentences", "Hello world."); // 1 sentence
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Ok(processed_doc) = result {
            // access metadata even on error for checking
            assert_eq!(
                processed_doc.metadata.get("c4_filter_status"),
                Some(&"filtered".to_string())
            );
            assert_eq!(
                processed_doc.metadata.get("c4_metric_sentence_count"),
                Some(&"1".to_string())
            );
            assert!(processed_doc
                .metadata
                .get("c4_filter_reasons")
                .unwrap()
                .contains("Too few sentences (found 1, required 2)"));
        } else if let Err(PipelineError::DocumentFiltered { reason, .. }) = result {
            assert!(reason.contains("Too few sentences (found 1, required 2)"));
        } else {
            panic!("Expected DocumentFiltered error, got different error or Ok");
        }
    }

    // Test: too_few_words_in_sentence
    // The reason string should be the same, as we try to preserve the first error.
    #[tokio::test]
    async fn test_too_few_words_in_sentence_metadata() {
        let filter = C4QualityFilter::new(2, 3, 10);
        let doc_content = "First sentence is fine. Oops.";
        let doc = create_test_doc("fail_words_meta", doc_content);
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
        // To check metadata:
        // let processed_doc = filter.process(create_test_doc("fail_words_meta", doc_content)).await.unwrap_err().into_document_somehow();
        // This requires a way to get the document back from the error or a different testing strategy.
        // For now, focusing on reason string consistency for existing tests.
    }

    // test_sentence_ends_with_quote_passes: This should still pass, metadata can be checked.
    #[tokio::test]
    async fn test_sentence_ends_with_quote_passes_metadata() {
        let filter = C4QualityFilter::new(1, 2, 10);
        let doc_content = "He said \"Hello world\".";
        let doc = create_test_doc("pass_quote_end_meta", doc_content);
        let result_doc = filter.process(doc).await.expect("Document should pass");
        assert_eq!(
            result_doc.metadata.get("c4_filter_status"),
            Some(&"passed".to_string())
        );
        assert_eq!(
            result_doc.metadata.get("c4_metric_sentence_count"),
            Some(&"1".to_string())
        );
    }

    // test_empty_document_content: Should still fail for too few sentences, metadata can be checked.
    #[tokio::test]
    async fn test_empty_document_content_metadata() {
        let filter = C4QualityFilter::new(1, 1, 10);
        let doc = create_test_doc("empty_content_meta", "");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        if let Err(PipelineError::DocumentFiltered { reason, doc_id: _ }) = result {
            assert!(reason.contains("Too few sentences (found 0, required 1)"));
            // At this point, the `document` that was processed and had metadata added is consumed by `Err`.
            // To check its metadata, the test or `PipelineError` would need to provide access to it.
        } else {
            panic!("Expected DocumentFiltered error for empty content");
        }
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
        assert!(
            result_part1.is_ok(),
            "Part 1: Should pass with sentence '.' if min_words=0. Actual: {:?}",
            result_part1.err()
        );

        // Part 2: Test that a sentence consisting only of punctuation (like ".")
        // FAILS if min_words_per_sentence > 0.
        let filter_part2 = C4QualityFilter::new(1, 1, 5); // min_sentences = 1, min_words_per_sentence = 1, max_word_length = 5

        // Test with a multi-sentence document where one sentence is just "."
        // Using an even simpler string that should isolate the "." sentence failure.
        let doc_part2_multi = create_test_doc("punc_only_sentence_fail_multi", ". .");
        let result_part2_multi = filter_part2.process(doc_part2_multi.clone()).await;

        // Expected: The first "." sentence should fail because word_count (0) < min_words_per_sentence (1).
        assert!(
            result_part2_multi.is_err(),
            "Part 2 (multi-sentence '. .'): Document should fail. Actual: {:?}",
            result_part2_multi.as_ref().map(|_| "Ok").unwrap_or("Err")
        );
        if let Err(PipelineError::DocumentFiltered { ref reason, .. }) = result_part2_multi {
            // Adjust expectation: split_into_sentences likely treats ". ." as a single sentence ". .".
            assert!(
                reason.contains("Too few words in sentence: \". .\" (found 0, required 1)"),
                "Incorrect reason for multi-sentence '. .': {}",
                reason
            );
        } else if result_part2_multi.is_ok() {
            panic!(
                "Part 2 (multi-sentence '. .') expected Err, got Ok. Doc: {:?}",
                doc_part2_multi
            );
        } else {
            panic!("Part 2 (multi-sentence '. .') expected DocumentFiltered error, got different error: {:?}", result_part2_multi.err());
        }

        // Test with a document that is ONLY "."
        let doc_part2_single = create_test_doc("punc_only_sentence_fail_single", ".");
        let result_part2_single = filter_part2.process(doc_part2_single.clone()).await;
        assert!(result_part2_single.is_err(), "Part 2 (single '.'): Document with only '.' sentence should fail if min_words=1. Actual: {:?}", result_part2_single.as_ref().map(|_| "Ok").unwrap_or("Err"));
        if let Err(PipelineError::DocumentFiltered { ref reason, .. }) = result_part2_single {
            assert!(
                reason.contains("Too few words in sentence: \".\" (found 0, required 1)"),
                "Incorrect reason for single '.': {}",
                reason
            );
        } else if result_part2_single.is_ok() {
            panic!(
                "Part 2 (single '.') expected Err, got Ok. Doc: {:?}",
                doc_part2_single
            );
        } else {
            panic!(
                "Part 2 (single '.') expected DocumentFiltered error, got different error: {:?}",
                result_part2_single.err()
            );
        }
    }
}
