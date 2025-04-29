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
        let text = &document.content;
        let words = split_into_words(text);
        let n_words = words.len().max(1); // avoid div by zero

        // Filter out pure-symbol tokens
        let non_symbol_words: Vec<&str> = words
            .iter()
            .filter(|w_ref_ref| w_ref_ref.chars().any(|c| !PUNCTUATION.contains(&c)))
            .map(|w_ref_ref| *w_ref_ref) // Dereference &&str to &str
            .collect();
        let n_non_sym = non_symbol_words.len();

        // Word count bounds
        if let Some(min) = self.min_doc_words {
            if n_non_sym < min {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_short_doc".into(),
                });
            }
        }
        if let Some(max) = self.max_doc_words {
            if n_non_sym > max {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_long_doc".into(),
                });
            }
        }

        // Average word length
        if !non_symbol_words.is_empty() {
            let sum_len: usize = non_symbol_words.iter().map(|w| w.len()).sum();
            let avg_len = sum_len as f64 / non_symbol_words.len() as f64;
            if let Some(min_avg) = self.min_avg_word_length {
                if avg_len < min_avg {
                    return Err(PipelineError::DocumentFiltered {
                        doc_id: document.id.clone(),
                        reason: "gopher_below_avg_threshold".into(),
                    });
                }
            }
            if let Some(max_avg) = self.max_avg_word_length {
                if avg_len > max_avg {
                    return Err(PipelineError::DocumentFiltered {
                        doc_id: document.id.clone(),
                        reason: "gopher_above_avg_threshold".into(),
                    });
                }
            }
        }

        // Symbol-to-word ratios (# and ellipsis)
        if let Some(max_sym) = self.max_symbol_word_ratio {
            let hash_ratio = text.chars().filter(|&c| c == '#').count() as f64 / n_words as f64;
            if hash_ratio > max_sym {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_too_many_hashes".into(),
                });
            }
            let ellipsis_count = text.matches("...").count() + text.matches("…").count();
            let ell_ratio = ellipsis_count as f64 / n_words as f64;
            if ell_ratio > max_sym {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_too_many_ellipsis".into(),
                });
            }
        }

        // Line-based bullet/ellipsis ratios
        let lines: Vec<&str> = text.lines().collect();
        let n_lines = lines.len().max(1);
        if let Some(max_bul) = self.max_bullet_lines_ratio {
            let bullet_lines = lines
                .iter()
                .filter(|&&l| {
                    let ls = l.trim_start();
                    ls.starts_with('•') || ls.starts_with('-')
                })
                .count();
            if bullet_lines as f64 / n_lines as f64 > max_bul {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_too_many_bullets".into(),
                });
            }
        }
        if let Some(max_ell) = self.max_ellipsis_lines_ratio {
            let ell_lines = lines
                .iter()
                .filter(|&&l| {
                    let le = l.trim_end();
                    le.ends_with("...") || le.ends_with("…")
                })
                .count();
            if ell_lines as f64 / n_lines as f64 > max_ell {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_too_many_end_ellipsis".into(),
                });
            }
        }

        // Alphabetic-word ratio
        // Check if the ratio of non-alphabetic words exceeds the maximum allowed ratio.
        // This is equivalent to checking if the ratio of alphabetic words is below
        // the minimum required ratio (1.0 - max_non_alpha_words_ratio).
        if let Some(max_non_alpha) = self.max_non_alpha_words_ratio {
            let alpha_count = words
                .iter()
                .filter(|w| w.chars().any(|c| c.is_alphabetic()))
                .count();
            // Filter if the ratio of alphabetic words is too low
            if (alpha_count as f64) / (n_words as f64) < (1.0 - max_non_alpha) {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_below_alpha_threshold".into(), // Document has too many non-alphabetic words
                });
            }
        }

        // Stop-word presence
        if let Some(min_sw) = self.min_stop_words {
            let sw_count = words
                .iter()
                .filter(|w| self.stop_words.contains(**w))
                .count();
            if sw_count < min_sw {
                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: "gopher_enough_stop_words".into(),
                });
            }
        }

        Ok(document)
    }
}
