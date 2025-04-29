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
            let trimmed = sentence.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Words per sentence
            let words = split_into_words(trimmed);
            let word_count = words.len();
            if word_count < self.min_words_per_sentence {
                let reason = format!(
                    "Too few words in sentence (found {}, required {})",
                    word_count, self.min_words_per_sentence
                );
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason,
                });
            }

            // Terminal punctuation
            if let Some(last_char) = trimmed.chars().last() {
                let valid_end = matches!(last_char, '.' | '!' | '?' | '"' | '\'');
                if !valid_end {
                    let reason = format!(
                        "Sentence does not end with punctuation (sentence: \"{}\", last_char: '{}')",
                        trimmed, last_char
                    );
                    return Err(PipelineError::DocumentFiltered {
                        doc_id: document.id.clone(),
                        reason,
                    });
                }
            }
        }

        // Check maximum word length across all content
        let all_words = split_into_words(&document.content);
        for word in all_words {
            if word.chars().count() > self.max_word_length {
                let reason = format!(
                    "Word exceeds max length (word: \"{}\", length: {}, max: {})",
                    word,
                    word.len(),
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
