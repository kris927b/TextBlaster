use async_trait::async_trait;
use regex::Regex;
use std::collections::{HashMap, HashSet};

use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result}; // Use your Result type alias
use crate::executor::ProcessingStep; // Assuming ProcessingStep trait is here or imported
use crate::utils::text::split_into_words;

/// Generate all contiguous n-grams of words, joined by spaces.
fn get_n_grams(words: &[&str], n: usize) -> Vec<String> {
    words.windows(n).map(|window| window.join(" ")).collect()
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
    let mut counter: HashMap<String, usize> = HashMap::new();
    for elem in items {
        *counter.entry(elem.to_string().clone()).or_insert(0) += 1;
    }
    if let Some((gram, count)) = counter.into_iter().max_by_key(|&(_, cnt)| cnt) {
        gram.len() * count
    } else {
        0
    }
}

/// Slide over words in steps, summing lengths of duplicate n-grams (concatenated without spaces).
fn find_all_duplicate(words: &[&str], n: usize) -> usize {
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
        let text = &document.content;

        // Paragraph checks
        let paragraphs: Vec<String> = self
            .paragraph_exp
            .split(text.trim())
            .map(str::to_string)
            .collect();
        let (para_dup_elems, para_dup_chars) = find_duplicates(&paragraphs);
        if let Some(threshold) = self.dup_para_frac {
            if !paragraphs.is_empty()
                && (para_dup_elems as f64 / paragraphs.len() as f64) > threshold
            {
                let reason = "dup_para_frac".into();

                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: reason,
                });
            }
        }
        if let Some(threshold) = self.dup_para_char_frac {
            if (para_dup_chars as f64 / text.len() as f64) > threshold {
                let reason = "dup_para_char_frac".into();

                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: reason,
                });
            }
        }

        // Line checks
        let lines: Vec<String> = self.line_splitter.split(text).map(str::to_string).collect();
        let (line_dup_elems, line_dup_chars) = find_duplicates(&lines);
        if let Some(threshold) = self.dup_line_frac {
            if !lines.is_empty() && (line_dup_elems as f64 / lines.len() as f64) > threshold {
                let reason = "dup_line_frac".into();

                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: reason,
                });
            }
        }
        if let Some(threshold) = self.dup_line_char_frac {
            if (line_dup_chars as f64 / text.len() as f64) > threshold {
                let reason = "dup_line_char_frac".into();

                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: reason,
                });
            }
        }

        // Word-based n-grams
        let words = split_into_words(text);
        for &(n, frac) in &self.top_n_grams {
            let n_grams = get_n_grams(&words, n);
            if !n_grams.is_empty() {
                let top_len = find_top_duplicate(&n_grams);
                if (top_len as f64 / text.len() as f64) > frac {
                    let reason = format!("top_{}_gram", n);

                    return Err(PipelineError::DocumentFiltered {
                        doc_id: document.id.clone(),
                        reason: reason,
                    });
                }
            }
        }
        for &(n, frac) in &self.dup_n_grams {
            let dup_chars = find_all_duplicate(&words, n);
            if (dup_chars as f64 / text.len() as f64) > frac {
                let reason = format!("duplicated_{}_n_grams", n);

                return Err(PipelineError::DocumentFiltered {
                    doc_id: document.id.clone(),
                    reason: reason,
                });
            }
        }

        Ok(document)
    }
}
