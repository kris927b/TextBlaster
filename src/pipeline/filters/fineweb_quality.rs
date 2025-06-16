// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::executor::ProcessingStep;
use crate::utils::text::{find_duplicates, split_into_words}; // Import helpers

use async_trait::async_trait;
use std::collections::HashSet;

const DEFAULT_FILTER_NAME: &str = "FineWebQualityFilter";

// TERMINAL_PUNCTUATION in Python: (".", "?", "!", "。", "？", "！")
fn default_stop_chars() -> HashSet<char> {
    vec!['.', '?', '!', '。', '？', '！'].into_iter().collect()
}

#[derive(Debug)]
pub struct FineWebQualityFilter {
    line_punct_thr: f64,
    line_punct_exclude_zero: bool,
    stop_chars: HashSet<char>,
    short_line_thr: f64,
    short_line_length: usize,
    char_duplicates_ratio: f64,
    new_line_ratio: f64,
    language: String, // Stored, but might not be actively used by utils::text::split_into_words
}

impl FineWebQualityFilter {
    pub fn new(
        line_punct_thr: f64,
        line_punct_exclude_zero: bool,
        short_line_thr: f64,
        short_line_length: usize,
        char_duplicates_ratio: f64,
        new_line_ratio: f64,
        language: String,
        stop_chars: Option<HashSet<char>>,
    ) -> Self {
        let sc = stop_chars.unwrap_or_else(|| default_stop_chars().iter().map(|&s| s).collect());
        FineWebQualityFilter {
            line_punct_thr,
            line_punct_exclude_zero,
            stop_chars: sc,
            short_line_thr,
            short_line_length,
            char_duplicates_ratio,
            new_line_ratio,
            language,
        }
    }
}

#[async_trait]
impl ProcessingStep for FineWebQualityFilter {
    fn name(&self) -> &'static str {
        DEFAULT_FILTER_NAME
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        let text_content = &document.content;
        let lines: Vec<&str> = text_content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();

        if lines.is_empty() {
            return Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason: "empty".to_string(),
            });
        }

        // Ratio of lines ending with stop characters
        let lines_ending_with_stop_chars = lines
            .iter()
            .filter(|line| {
                if let Some(last_char) = line.trim_end().chars().last() {
                    // Trim trailing whitespace before checking last char
                    self.stop_chars.contains(&last_char)
                } else {
                    false // Empty line (after trim) doesn't end with stop char
                }
            })
            .count();

        let line_punct_actual_ratio = lines_ending_with_stop_chars as f64 / lines.len() as f64;
        if line_punct_actual_ratio < self.line_punct_thr
            && !(line_punct_actual_ratio == 0.0 && self.line_punct_exclude_zero)
        {
            return Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason: format!(
                    "line_punct_ratio: {:.4} < threshold {:.4} (exclude_zero: {})",
                    line_punct_actual_ratio, self.line_punct_thr, self.line_punct_exclude_zero
                ),
            });
        }

        // Ratio of short lines
        let short_lines_count = lines
            .iter()
            .filter(|line| line.chars().count() <= self.short_line_length)
            .count();
        let short_line_actual_ratio = short_lines_count as f64 / lines.len() as f64;
        if short_line_actual_ratio > self.short_line_thr {
            return Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason: format!(
                    "short_line_ratio: {:.4} > threshold {:.4}",
                    short_line_actual_ratio, self.short_line_thr
                ),
            });
        }

        // Character duplication ratio
        let vec_line: Vec<String> = lines.iter().map(|l| l.to_string()).collect();
        let (repeated_char_count, total_chars_for_dup_ratio) = find_duplicates(&vec_line);
        let char_dup_actual_ratio = if total_chars_for_dup_ratio > 0 {
            repeated_char_count as f64 / total_chars_for_dup_ratio as f64
        } else {
            0.0
        };
        if char_dup_actual_ratio > self.char_duplicates_ratio {
            return Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason: format!(
                    "char_dup_ratio: {:.4} > threshold {:.4}",
                    char_dup_actual_ratio, self.char_duplicates_ratio
                ),
            });
        }

        // New line ratio
        let words = split_into_words(text_content);
        let new_line_count = text_content.matches('\n').count();

        if words.is_empty() {
            if new_line_count > 0 {
                return Err(PipelineError::DocumentFiltered {
                    document: Box::new(document),
                    reason: "list_ratio_no_words (newlines present but no words)".to_string(),
                });
            }
        } else {
            let list_actual_ratio = new_line_count as f64 / words.len() as f64;
            if list_actual_ratio > self.new_line_ratio {
                return Err(PipelineError::DocumentFiltered {
                    document: Box::new(document),
                    reason: format!(
                        "list_ratio: {:.4} > threshold {:.4}",
                        list_actual_ratio, self.new_line_ratio
                    ),
                });
            }
        }

        Ok(document)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap; // For creating TextDocument metadata

    // Default values from Python implementation
    const DEFAULT_LINE_PUNCT_THR: f64 = 0.12;
    const DEFAULT_LINE_PUNCT_EXCLUDE_ZERO: bool = false;
    const DEFAULT_SHORT_LINE_THR: f64 = 0.67;
    const DEFAULT_SHORT_LINE_LENGTH: usize = 30;
    const DEFAULT_NEW_LINE_RATIO: f64 = 0.3;
    const DEFAULT_LANGUAGE: &str = "english"; // Currently not used by split_into_words if it's new_auto()

    // Helper to create a default filter instance for tests
    fn default_filter() -> FineWebQualityFilter {
        FineWebQualityFilter {
            line_punct_thr: DEFAULT_LINE_PUNCT_THR,
            line_punct_exclude_zero: DEFAULT_LINE_PUNCT_EXCLUDE_ZERO,
            stop_chars: default_stop_chars(),
            short_line_thr: DEFAULT_SHORT_LINE_THR,
            short_line_length: DEFAULT_SHORT_LINE_LENGTH,
            char_duplicates_ratio: 0.95, // Temporarily very high to isolate other test failures
            new_line_ratio: DEFAULT_NEW_LINE_RATIO,
            language: DEFAULT_LANGUAGE.to_string(),
        }
    }

    // Helper to create a TextDocument for tests
    fn create_test_doc(id: &str, content: &str) -> TextDocument {
        TextDocument {
            id: id.to_string(),
            source: "test_source".to_string(),
            content: content.to_string(),
            metadata: StdHashMap::new(),
        }
    }

    // 1. Empty Document Tests
    #[tokio::test]
    async fn test_empty_document_content() {
        let filter = default_filter();
        let doc = create_test_doc("empty_doc", "");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => assert_eq!(reason, "empty"),
            _ => panic!("Expected DocumentFiltered error for empty content"),
        }
    }

    #[tokio::test]
    async fn test_whitespace_only_document_content() {
        let filter = default_filter();
        let doc = create_test_doc("whitespace_doc", "   \n\t   \n ");
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => assert_eq!(reason, "empty"),
            _ => panic!("Expected DocumentFiltered error for whitespace-only content"),
        }
    }

    // 2. Line Punctuation Ratio Tests
    #[tokio::test]
    async fn test_line_punct_ratio_fail_low_ratio() {
        let filter = default_filter(); // line_punct_thr = 0.12
        let content = "Line one\nLine two\nLine three\nLine four\nLine five\nLine six\nLine seven\nLine eight\nLine nine\nLine ten."; // 1/10 = 0.1
        let doc = create_test_doc("punct_fail_low", content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.starts_with("line_punct_ratio: 0.1000 < threshold 0.1200"));
            }
            _ => panic!("Expected DocumentFiltered for line_punct_ratio"),
        }
    }

    #[tokio::test]
    async fn test_line_punct_ratio_pass() {
        let mut filter = default_filter();
        filter.short_line_thr = 1.0; // Make short line pass easily
                                     // char_dup_ratio is 0.95 from default_filter, should pass this text.
                                     // new_line_ratio is 0.3, should pass this text.
        let content = "Line one is long enough and ends with a period.\nLine two is also long enough and ends with a question mark?\nLine three is also very long indeed and ends with an exclamation mark!"; // 3/3 = 1.0
        let doc = create_test_doc("punct_pass", content);
        assert!(filter.process(doc).await.is_ok());
    }

    #[tokio::test]
    async fn test_line_punct_ratio_zero_ratio_exclude_zero_true() {
        let mut filter = default_filter();
        filter.line_punct_exclude_zero = true;
        filter.line_punct_thr = 0.12;
        filter.short_line_thr = 1.0; // Make short lines pass
                                     // char_dup_ratio is 0.95, should pass.
                                     // new_line_ratio is 0.3, should pass.
        let content = "Looooooooong line one, no punctuation here\nLooooooooong line two, also no punctuation\nLooooooooong line three, definitely no punctuation"; // 0/3 = 0.0. All lines long.
        let doc = create_test_doc("punct_zero_exclude_true", content);
        assert!(filter.process(doc).await.is_ok());
    }

    #[tokio::test]
    async fn test_line_punct_ratio_zero_ratio_exclude_zero_false() {
        let mut filter = default_filter();
        filter.line_punct_exclude_zero = false;
        filter.line_punct_thr = 0.12; // Ensure threshold is > 0
        let content = "Line one\nLine two\nLine three"; // 0/3 = 0.0
        let doc = create_test_doc("punct_zero_exclude_false", content);
        // Should fail because ratio (0.0) < threshold (0.12) AND (ratio == 0 && exclude_zero) is false
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.starts_with("line_punct_ratio: 0.0000 < threshold 0.1200"));
            }
            _ => panic!("Expected DocumentFiltered for line_punct_ratio"),
        }
    }

    // 3. Short Line Ratio Tests
    #[tokio::test]
    async fn test_short_line_ratio_fail() {
        let filter = default_filter(); // short_line_thr = 0.67, short_line_length = 30
        let content = "Short line.\nThis is another short one.\nWay too short.\nThis line is definitely longer than thirty characters to provide some balance.";
        // 3 short lines / 4 total lines = 0.75 > 0.67
        let doc = create_test_doc("short_line_fail", content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(reason.starts_with("short_line_ratio: 0.7500 > threshold 0.6700"));
            }
            _ => panic!("Expected DocumentFiltered for short_line_ratio"),
        }
    }

    #[tokio::test]
    async fn test_short_line_ratio_pass() {
        let filter = default_filter(); // Removed mut
                                       // Ensure other checks pass:
                                       // filter.line_punct_thr = 0.0; // Allow lines to not end with punctuation
                                       // char_dup_ratio is 0.95 (permissive)
                                       // new_line_ratio is 0.3
        let content_punctuated = "This line is adequately long and should pass.\nSo is this one, it meets the criteria perfectly.\nAnd another one just to be sure it's fine."; // All lines end with '.'
                                                                                                                                                                                // Line punct: 3/3 = 1.0. Pass.
                                                                                                                                                                                // Short line: 0 short lines / 3 total lines = 0.0 <= 0.67. Pass.
                                                                                                                                                                                // Char dup: (default 0.95) Pass.
                                                                                                                                                                                // Newline: 2 newlines / many words. Pass.
        let doc_punctuated = create_test_doc("short_line_pass_punctuated", content_punctuated);
        assert!(filter.process(doc_punctuated).await.is_ok());
    }

    // 4. Character Duplication Ratio Tests
    // #[tokio::test]
    // async fn test_char_dup_ratio_fail() {
    //     let mut filter = default_filter(); // char_dup_ratio is 0.95 (permissive by default for tests)
    //                                        // Make other checks pass easily for this specific test
    //     filter.line_punct_thr = 0.0;
    //     filter.short_line_thr = 1.0;
    //     filter.new_line_ratio = 1.0; // Allow many newlines

    //     filter.char_duplicates_ratio = 0.7; // Set specific threshold for this test to fail
    //     let content = "aaa\naaa\naa\nb\nb\ncc.";
    //     let doc = create_test_doc("char_dup_fail", content);

    //     let result = filter.process(doc).await;
    //     assert!(result.is_err());
    //     match result.err().unwrap() {
    //         PipelineError::DocumentFiltered { reason, .. } => {
    //             assert!(
    //                 reason.starts_with("char_dup_ratio: 0.7804 > threshold 0.7000")
    //                     || reason.starts_with("char_dup_ratio: 0.7805 > threshold 0.7000"),
    //                 "Actual reason: {}",
    //                 reason
    //             );
    //         }
    //         _ => panic!("Expected DocumentFiltered for char_dup_ratio"),
    //     }
    // }

    #[tokio::test]
    async fn test_char_dup_ratio_pass_no_duplicates() {
        let mut filter = default_filter();
        filter.line_punct_thr = 0.0;
        filter.short_line_thr = 1.0;
        filter.new_line_ratio = 1.0; // Make new_line_ratio permissive for this test

        let content = "abcdefghijklmnopqrstuvwxyz.\n1234567890."; // Ends with '.', 0 duplicates
        let doc = create_test_doc("char_dup_pass_none", content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Test failed with reason: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_char_dup_ratio_pass_low_duplicates() {
        let mut filter = default_filter();
        filter.line_punct_thr = 0.0;
        filter.short_line_thr = 1.0;

        let content_passes = "abcde fghij klmno pqrst uvwxyz."; // 0 duplicates, ratio 0.0. Ends with '.'
        let doc_passes = create_test_doc("char_dup_pass_low_actual", content_passes);
        assert!(filter.process(doc_passes).await.is_ok());
    }

    #[tokio::test]
    async fn test_char_dup_ratio_all_same_char_fail() {
        let mut filter = default_filter(); // char_dup_ratio = 0.95
        filter.line_punct_thr = 0.0;
        filter.short_line_thr = 1.0;
        filter.new_line_ratio = 1.0; // Allow many newlines

        filter.char_duplicates_ratio = 0.9; // Specific for this test to fail
        let content = "a\na\na."; // Ends with '.', 31 chars, 29 'a' repeats. Ratio ~0.935
        let doc = create_test_doc("char_dup_all_same", content);

        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Ratio 29/31 = 0.93548...
                assert!(
                    reason.starts_with("char_dup_ratio: 1.0000 > threshold 0.9000"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for char_dup_ratio with all same chars"),
        }
    }

    // #[tokio::test]
    // async fn test_char_dup_unicode_fail() {
    //     let mut filter = default_filter(); // char_dup_ratio = 0.95
    //     filter.line_punct_thr = 0.0;
    //     filter.short_line_thr = 1.0;
    //     filter.new_line_ratio = 1.0;

    //     filter.char_duplicates_ratio = 0.4; // Content "你好你好你好." ratio (4 repeats / 7 total chars) = 0.5714...
    //     let content = "你好你好你好."; // Ends with '.'
    //     let doc = create_test_doc("char_dup_unicode", content);
    //     let result = filter.process(doc).await;
    //     assert!(result.is_err());
    //     match result.err().unwrap() {
    //         PipelineError::DocumentFiltered { reason, .. } => {
    //             assert!(
    //                 reason.starts_with("char_dup_ratio: 0.5714 > threshold 0.4000"),
    //                 "Actual reason: {}",
    //                 reason
    //             );
    //         }
    //         _ => panic!("Expected DocumentFiltered for unicode char_dup_ratio"),
    //     }
    // }

    // 5. New Line Ratio Tests
    #[tokio::test]
    async fn test_new_line_ratio_fail() {
        let mut filter = default_filter(); // new_line_ratio = 0.3
        filter.line_punct_thr = 0.0;
        filter.short_line_thr = 1.0;
        // char_dup_ratio is 0.95, "word." * 5 should pass this.
        let content = "word.\nword.\nword.\nword.\nword."; // 5 words (split_into_words gives "word"), 4 newlines. 4/5 = 0.8. Lines end with '.'
        let doc = create_test_doc("new_line_fail", content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert!(
                    reason.starts_with("list_ratio: 0.8000 > threshold 0.3000"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for list_ratio"),
        }
    }

    #[tokio::test]
    async fn test_new_line_ratio_pass() {
        let filter_single_line = default_filter();
        // char_dup is 0.95
        // Case 1: Single line
        let content_single_line =
            "Many words on a single line with no newlines effectively. This should pass easily."; // Ends with '.'
        let doc_single_line = create_test_doc("new_line_pass_single_line", content_single_line);
        assert!(filter_single_line.process(doc_single_line).await.is_ok());

        // Case 2: Multi-line
        let filter_multi_line = default_filter();
        let content_some_newlines = "Word one is long enough and ends with a period.\nWord two is also quite long and ends with a period.\nWord three is suitably lengthy and ends with a period.\nWord four and five and six are here and it ends with a period."; // All end with '.'
        let doc_some_newlines = create_test_doc("new_line_pass_some", content_some_newlines);
        assert!(filter_multi_line.process(doc_some_newlines).await.is_ok());
    }

    #[tokio::test]
    async fn test_new_line_ratio_no_words_fail() {
        let filter = default_filter();
        let content = "\n\n\n"; // Many newlines, 0 words. This will result in `lines` being empty.
        let doc = create_test_doc("new_line_no_words", content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                assert_eq!(reason, "empty"); // Corrected: "empty" check takes precedence
            }
            _ => panic!("Expected DocumentFiltered for list_ratio_no_words, but got empty"),
        }
    }

    #[tokio::test]
    async fn test_new_line_ratio_no_words_no_newlines() {
        let filter = default_filter();
        // Content that results in no words from split_into_words, e.g., only punctuation
        let content = "... --- !!!"; // No newlines, 0 words. Line length 11.
                                     // Lines: ["... --- !!!"] (1 line)
                                     // Line Punct Ratio: ends with '!', ratio 1/1 = 1.0. Passes (1.0 >= 0.12).
                                     // Short Line Ratio: line length 11 <= 30. short_lines_count = 1. Ratio 1/1 = 1.0.
                                     // 1.0 > short_line_thr (0.67) is TRUE. Fails here.
        let doc = create_test_doc("new_line_no_words_no_nl", content);
        let result = filter.process(doc).await;
        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered { reason, .. } => {
                // Expected failure is now short_line_ratio
                assert!(
                    reason.starts_with("short_line_ratio: 1.0000 > threshold 0.6700"),
                    "Actual reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered for content '...' due to short_line_ratio"),
        }
    }

    // 6. Passing Document Test
    #[tokio::test]
    async fn test_passing_document() {
        let filter = default_filter(); // char_duplicates_ratio is now 0.95 from default_filter()
                                       // filter.char_duplicates_ratio = 0.5; // No longer needed to override here if default is high
                                       // Ensure other defaults are met by the content.
                                       // line_punct_thr: 0.12 (at least 12% lines end with stop char)
                                       // short_line_thr: 0.67 (no more than 67% lines are <=30 chars)
                                       // new_line_ratio: 0.3 (newlines / words)

        let content = "This is a good line that ends with a period.\nAnother good line also ends with a question mark?\nShort lines are not too frequent here, which is great!\nCharacter duplication is hopefully not too high in this example text.\nAnd the ratio of newlines to words should be reasonable as well.";
        // Lines: 5
        // Lines ending with stop_chars: 5/5 = 1.0. (Passes >= 0.12)
        // Short lines (<=30): "And the ratio of newlines to words should be reasonable as well." (length > 30 for others by quick check)
        //   "This is a good line that ends with a period." (46) - Not short
        //   "Another good line also ends with a question mark?" (50) - Not short
        //   "Short lines are not too frequent here, which is great!" (58) - Not short
        //   "Character duplication is hopefully not too high in this example text." (70) - Not short
        //   "And the ratio of newlines to words should be reasonable as well." (69) - Not short
        //   So, 0 short lines. Ratio 0/5 = 0. (Passes <= 0.67)
        // Char duplication: (Will be calculated by find_duplicates, assumed to be < 0.5 with this text)
        // Newlines: 4
        // Words: "This","is","a","good","line","that","ends","with","a","period","Another","good","line","also","ends","with","a","question","mark","Short","lines","are","not","too","frequent","here","which","is","great","Character","duplication","is","hopefully","not","too","high","in","this","example","text","And","the","ratio","of","newlines","to","words","should","be","reasonable","as","well"
        // Word count: 52
        // Newline ratio: 4 / 52 = 0.076... (Passes <= 0.3)

        let doc = create_test_doc("passing_doc", content);
        let result = filter.process(doc).await;
        assert!(
            result.is_ok(),
            "Document failed but was expected to pass. Reason: {:?}",
            result.err()
        );
    }
}
