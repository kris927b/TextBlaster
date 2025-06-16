// Text utils
// use itertools::Itertools; // Removed unused import

use icu::segmenter::{SentenceSegmenter, WordSegmenter};
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};

//Danish Stopwords
pub const DANISH_STOP_WORDS: &[&str] = &[
    "ad", "af", "aldrig", "alle", "alt", "anden", "andet", "andre", "at", "bare", "begge", "blev",
    "blive", "bliver", "da", "de", "dem", "den", "denne", "der", "deres", "det", "dette", "dig",
    "din", "dine", "disse", "dit", "dog", "du", "efter", "ej", "eller", "en", "end", "ene",
    "eneste", "enhver", "er", "et", "far", "fem", "fik", "fire", "flere", "fleste", "for", "fordi",
    "forrige", "fra", "få", "får", "før", "god", "godt", "ham", "han", "hans", "har", "havde",
    "have", "hej", "helt", "hende", "hendes", "her", "hos", "hun", "hvad", "hvem", "hver",
    "hvilken", "hvis", "hvor", "hvordan", "hvorfor", "hvornår", "i", "ikke", "ind", "ingen",
    "intet", "ja", "jeg", "jer", "jeres", "jo", "kan", "kom", "komme", "kommer", "kun", "kunne",
    "lad", "lav", "lidt", "lige", "lille", "man", "mand", "mange", "med", "meget", "men", "mens",
    "mere", "mig", "min", "mine", "mit", "mod", "må", "ned", "nej", "ni", "nogen", "noget",
    "nogle", "nu", "ny", "nyt", "når", "nær", "næste", "næsten", "og", "også", "okay", "om", "op",
    "os", "otte", "over", "på", "se", "seks", "selv", "ser", "ses", "sig", "sige", "sin", "sine",
    "sit", "skal", "skulle", "som", "stor", "store", "syv", "så", "sådan", "tag", "tage", "thi",
    "ti", "til", "to", "tre", "ud", "under", "var", "ved", "vi", "vil", "ville", "vor", "vores",
    "være", "været",
];

/// Exactly the same literal punctuation characters as in your Python string
const PUNCTUATION_LIT: &str =
    "!/—”:％１〈&(、━\\【#%「」，】；+^]~“《„';’{|∶´[=-`*．（–？！：$～«〉,><》)?）。…@_.\"}►»";

/// Ranges of codepoints to include (0..9, 11..13, 13..32, 127..160)
const PUNCTUATION_RANGES: &[(u32, u32)] = &[
    (0, 9),     // 0–8
    (11, 13),   // 11–12
    (13, 32),   // 13–31
    (127, 160), // 127–159
];

/// A lazily-initialized, global set of all punctuation chars
pub static PUNCTUATION: Lazy<HashSet<char>> = Lazy::new(|| {
    let mut set = HashSet::new();

    // 1) add all the literal chars
    set.extend(PUNCTUATION_LIT.chars());

    // 2) add every codepoint in each specified range
    for &(start, end) in PUNCTUATION_RANGES {
        for cp in start..end {
            // `from_u32` is safe here because all cp’s in 0..160 are valid Unicode Scalars
            if let Some(ch) = std::char::from_u32(cp) {
                set.insert(ch);
            }
        }
    }

    set
});

pub fn split_into_sentences(text: &str) -> Vec<&str> {
    // First, trim the input string to handle leading/trailing whitespace
    // and to simplify logic for "empty" or "all-whitespace" inputs.
    let trimmed_text = text.trim();
    if trimmed_text.is_empty() {
        return Vec::new();
    }

    let segmenter = SentenceSegmenter::new(); // Assuming Danish - adjust locale as needed
                                              // Perform segmentation on the already trimmed text
    let start_indices: Vec<usize> = segmenter.segment_str(trimmed_text).collect();

    // If the segmenter returns no start indices for a non-empty trimmed string,
    // it means no sentence breaks were found. In this case,
    // treat the entire trimmed string as a single sentence.
    if start_indices.is_empty() {
        return vec![trimmed_text]; // This is the key change for "Hello world." -> vec!["Hello world."]
    }

    let mut sentences = Vec::new();
    for i in 0..start_indices.len() {
        let start = start_indices[i];
        // Determine the end of the current sentence based on the next start index
        // or the end of the (trimmed) input string.
        let end = if i + 1 < start_indices.len() {
            start_indices[i + 1]
        } else {
            trimmed_text.len()
        };

        // Ensure the slice is valid against trimmed_text.
        // start_indices are relative to trimmed_text.
        if start <= end && end <= trimmed_text.len() {
            // Slice from trimmed_text. A further trim might be redundant if segmenter is well-behaved
            // with already trimmed input, but kept for safety against unusual segmenter output.
            let sentence_slice = trimmed_text[start..end].trim();
            if !sentence_slice.is_empty() {
                sentences.push(sentence_slice);
            }
        }
    }
    sentences
}

pub fn split_into_words(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    let segmenter = WordSegmenter::new_auto(); // No need for mut if not calling methods on it post-iterator creation
    let mut words = Vec::new();
    let mut prev_break = 0;

    // segment_str() returns an iterator over break points (usize byte offsets).
    // A segment is the text between two consecutive break points.
    let breaks_iter = segmenter.segment_str(text).peekable();

    for current_break in breaks_iter {
        // while let Some(current_break) = breaks_iter.next() {
        if current_break > prev_break {
            let segment_candidate = &text[prev_break..current_break];

            // Trim whitespace first to handle cases like "word " before punctuation check
            let trimmed_segment = segment_candidate.trim();

            if !trimmed_segment.is_empty() {
                // Check if the trimmed segment consists *only* of punctuation.
                // The ICU segmenter should ideally segment "word." as "word" (word-like) and "." (not).
                // And "..." as not word-like.
                // If the segmenter gives us "word.", we need to decide if that's a "word".
                // The current UAX #29 definition of a word often includes trailing punctuation within the segment
                // if `WordSegmenter::is_word_like` was true for it.
                // The `WordSegmenter` should correctly identify "mid" and "dle" in "mid...dle" as word segments,
                // and "..." as a non-word segment.

                // The `segment_str` iterator itself only gives break points.
                // The boolean from the original code `(usize, bool)` is what we are missing.
                // Let's assume the segmenter correctly breaks "word" from "..." from "dle".
                // Then we just need to check if the segment is non-empty after light trimming.
                // A segment like "..." would be correctly identified if it's not empty and all its chars are punctuation.

                let mut contains_word_char = false;
                for ch in trimmed_segment.chars() {
                    if !PUNCTUATION.contains(&ch) && !ch.is_whitespace() {
                        // Check against our PUNCTUATION set
                        contains_word_char = true;
                        break;
                    }
                }

                if contains_word_char {
                    // If it contains at least one non-punctuation/non-whitespace char, consider it a word.
                    // Further trimming of just punctuation might be desired depending on exact requirements.
                    // For "mid...dle", we expect segments "mid", "...", "dle".
                    // "mid" -> contains_word_char = true.
                    // "..." -> contains_word_char = false (if all '.' are in PUNCTUATION).
                    // "dle" -> contains_word_char = true.
                    // This seems like a reasonable heuristic if `is_word_like` is not available directly with the offset.
                    words.push(trimmed_segment);
                }
            }
        }
        prev_break = current_break;
    }

    // Process the last segment (from the last break to the end of the string)
    if text.len() > prev_break {
        let segment_candidate = &text[prev_break..text.len()];
        let trimmed_segment = segment_candidate.trim();
        if !trimmed_segment.is_empty() {
            let mut contains_word_char = false;
            for ch in trimmed_segment.chars() {
                if !PUNCTUATION.contains(&ch) && !ch.is_whitespace() {
                    contains_word_char = true;
                    break;
                }
            }
            if contains_word_char {
                words.push(trimmed_segment);
            }
        }
    }
    words
}

/// Generate all contiguous n-grams of words, joined by spaces.
pub fn get_n_grams(words: &[&str], n: usize) -> Vec<String> {
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
pub fn find_duplicates(items: &[String]) -> (usize, usize) {
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
pub fn find_top_duplicate(items: &[String]) -> usize {
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
pub fn find_all_duplicate(words: &[&str], n: usize) -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_sentences_empty_and_simple() {
        assert_eq!(
            split_into_sentences(""),
            Vec::new() as Vec<&str>,
            "Test empty string"
        );
        assert_eq!(
            split_into_sentences("   "),
            Vec::new() as Vec<&str>,
            "Test string with only spaces"
        );

        // This is the previously failing assertion. With the fix, it should pass.
        assert_eq!(
            split_into_sentences("Hello world."),
            vec!["Hello world."],
            "Test single sentence with period"
        );

        assert_eq!(
            split_into_sentences("  Hello world.  "),
            vec!["Hello world."],
            "Test single sentence with period and surrounding spaces"
        );
        assert_eq!(
            split_into_sentences("Dette er en sætning."),
            vec!["Dette er en sætning."],
            "Test Danish single sentence with period"
        );
        assert_eq!(
            split_into_sentences("SingleWord"),
            vec!["SingleWord"],
            "Test single word, no punctuation"
        );
        assert_eq!(
            split_into_sentences("  SingleWord  "),
            vec!["SingleWord"],
            "Test single word, no punctuation, with spaces"
        );
    }

    #[test]
    fn test_split_sentences_multiple() {
        let text = "Første sætning. Anden sætning! Tredje sætning?";
        let expected = vec!["Første sætning.", "Anden sætning!", "Tredje sætning?"];
        assert_eq!(
            split_into_sentences(text),
            expected,
            "Test multiple sentences standard"
        );

        let text_with_spaces = "  Første sætning.   Anden sætning!  Tredje sætning?  ";
        assert_eq!(
            split_into_sentences(text_with_spaces),
            expected,
            "Test multiple sentences with varied spacing"
        );

        let text_mixed_punc = " Hello. How are you? Fine! ";
        assert_eq!(
            split_into_sentences(text_mixed_punc),
            vec!["Hello.", "How are you?", "Fine!"],
            "Test mixed punctuation with surrounding spaces"
        );

        let text_no_final_punc = "This is a sentence. This is another";
        assert_eq!(
            split_into_sentences(text_no_final_punc),
            vec!["This is a sentence.", "This is another"],
            "Test multiple sentences, last one no punctuation"
        );

        let text_no_final_punc_spaced = "  This is a sentence.   This is another  ";
        assert_eq!(
            split_into_sentences(text_no_final_punc_spaced),
            vec!["This is a sentence.", "This is another"],
            "Test multiple sentences, last one no punctuation, with spaces"
        );
    }

    #[test]
    fn test_split_words_empty_and_simple() {
        assert_eq!(split_into_words(""), Vec::new() as Vec<&str>);
        assert_eq!(split_into_words("hello"), vec!["hello"]);
        assert_eq!(split_into_words("hello world"), vec!["hello", "world"]);
    }

    #[test]
    fn test_split_words_with_punctuation() {
        assert_eq!(split_into_words("hello, world!"), vec!["hello", "world"]);
        assert_eq!(
            split_into_words("first. second; third?"),
            vec!["first", "second", "third"]
        );
        assert_eq!(split_into_words("...leading"), vec!["leading"]); // Based on is_word_like
        assert_eq!(split_into_words("trailing..."), vec!["trailing"]);
        // For "mid...dle", WordSegmenter with is_word_like() filter:
        // Segments might be: "mid", "...", "dle".
        // is_word_like filters out "...".
        // tuple_windows on ("mid", WORD), ("dle", WORD) [assuming "..." is (NON_WORD)]
        // This would map directly to ["mid", "dle"]
        // What if segmenter output is like: (offset_mid, WORD), (offset_..., NONE), (offset_dle, WORD)
        // .filter(|(_, (_, segment_type))| segment_type.is_word_like())
        // -> keeps (offset_mid, WORD), (offset_dle, WORD)
        // .tuple_windows()
        // -> ((offset_mid, WORD), (offset_dle, WORD))
        // .map(|((i, _), (j, _))| &text[i..j]) -> text[offset_mid .. offset_dle]
        // This map logic is for when `j` is the start of the *next* segment.
        // Example: text = "word1 word2"
        // segment_str -> iter_with_word_type -> [(0, WORD), (6, WORD)] (assuming "word1 " is one segment by offset)
        // or more likely: [(0, WORD), (5, NONE), (6, WORD)] for "word1", " ", "word2"
        // filter: [(0, WORD), (6, WORD)]
        // tuple_windows: [ ((0,WORD), (6,WORD)) ]
        // map: text[0..6] -> "word1 " -- incorrect.
        // The `split_into_words` has a similar issue to `split_into_sentences` if not careful.
        // `WordSegmenter::segment_str(text).iter_with_word_type()` returns `Iterator<Item = (usize, WordType)>`.
        // `usize` is the start offset, `WordType` indicates if it's word-like.
        // So `filter` is correct.
        // `tuple_windows()` on the filtered stream `[(idx1, type1), (idx2, type2), ...]`
        // gives `((idx_A, type_A), (idx_B, type_B))`.
        // `map(|((i, _), (j, _))| &text[i..j])` then takes `&text[idx_A .. idx_B]`.
        // This is correct if idx_B is the start of the *next* word, and we want text up to that point.
        // But it actually means we are taking the slice from start of word A to start of word B.
        // This will include intervening spaces/punctuation. This is not what we want.
        // Example: "hello, world"
        // iter_with_word_type: (0, WORD), (5, NONE), (6, NONE), (7, WORD) for "hello", ",", " ", "world"
        // filter: (0, WORD), (7, WORD)
        // tuple_windows: [ ((0,WORD), (7,WORD)) ]
        // map: &text[0..7] -> "hello, " -- incorrect.
        // It should be: collect offsets of word-like segments, then for each offset, find its end (start of next segment or string end).
        // Or rather, the segmenter should ideally give start and end of each segment.
        //
        // `WordSegmenter::segment_str(text)` returns `impl Iterator<Item = (usize, bool)>`
        // where `usize` is byte offset, `bool` is `is_word_like`.
        // The `iter_with_word_type()` is not standard on the result of `segment_str`.
        // Ah, `WordSegmenterBreakIteratorLatin1` (result of `segment_str`) has `iter_with_word_type()`.
        // This iterator yields `(usize, SegmenterWordType)`. `usize` is the START of the segment.
        //
        // The current code is:
        // .segment_str(text) -> yields (offset, is_word_like_bool)
        // .iter_with_word_type() -> This is not a method on the iterator from segment_str.
        // It must be that segment_str is being called on `WordSegmenter` not its iterator.
        // `WordSegmenter::segment_str` itself returns the iterator.
        // Let's assume `WordSegmenter::BreakIterator` (what `segment_str` returns) has `iter_with_word_type`.
        // Looking at `icu_segmenter-1.4.1/src/word.rs`, `WordSegmenter::segment_str` returns `WordBreakIteratorPotentiallyDirty<'s, 'l, S>`.
        // This struct `impl<'s, 'l, S: RuleBreakType<'s, 'l>> Iterator for WordBreakIteratorPotentiallyDirty<'s, 'l, S>`
        // with `Item = (usize, bool)`.
        // It does NOT have an `iter_with_word_type` method.
        // The code `segmenter.segment_str(text).iter_with_word_type()` will not compile as written.
        //
        // Perhaps it's meant to be `segmenter.segment_str(text)` which yields `(usize, bool)`
        // then `.filter(|(_, is_word_like)| *is_word_like)`
        // then `.map(|(offset, _)| offset)`. This gives start indices of words.
        // Then `tuple_windows` on these start indices.
        // `map(|(i, j)| &text[i..j])`. This would slice from start of word1 to start of word2.
        // This is still wrong as it includes space.
        //
        // The `split_into_words` function needs a more fundamental fix.
        // A correct approach: iterate through segments, if segment is word-like, take `&text[start_offset..end_offset]`.
        // `end_offset` is the start_offset of the *next* segment.
        //
        // For now, I'll test based on the *intended* behavior of `split_into_words`.
        // These tests might fail or highlight issues with the current implementation.
        assert_eq!(split_into_words("mid...dle"), vec!["mid", "dle"]); // Expected: "mid", "dle"
                                                                       // Current code (if it compiled & worked as described for sentences):
                                                                       // "mid...dle" -> segments "mid" (word), "..." (not word), "dle" (word)
                                                                       // Filtered starts: idx_mid, idx_dle
                                                                       // tuple_windows: (idx_mid, idx_dle)
                                                                       // slice: text[idx_mid .. idx_dle] -> "mid..." -- incorrect.
                                                                       // So this test will likely fail.
    }

    #[test]
    fn test_split_words_danish() {
        assert_eq!(split_into_words("hej med dig"), vec!["hej", "med", "dig"]);
        assert_eq!(split_into_words("en, to, tre!"), vec!["en", "to", "tre"]);
    }

    #[test]
    fn test_punctuation_set_contents() {
        assert!(PUNCTUATION.contains(&'.'));
        assert!(PUNCTUATION.contains(&','));
        assert!(PUNCTUATION.contains(&'!'));
        assert!(PUNCTUATION.contains(&'?'));
        assert!(PUNCTUATION.contains(&'"')); // From PUNCTUATION_LIT
        assert!(PUNCTUATION.contains(&'\u{0000}')); // From PUNCTUATION_RANGES (0,9) -> 0
        assert!(PUNCTUATION.contains(&'\u{001F}')); // From PUNCTUATION_RANGES (13,32) -> 31

        assert!(!PUNCTUATION.contains(&'a'));
        assert!(!PUNCTUATION.contains(&'A'));
        assert!(!PUNCTUATION.contains(&'5')); // Assuming '5' is not in ranges/literal
    }

    #[test]
    fn test_danish_stop_words_simple_check() {
        // Not much to test other than existence and maybe a few key values
        assert!(!DANISH_STOP_WORDS.is_empty());
        assert!(DANISH_STOP_WORDS.contains(&"og"));
        assert!(DANISH_STOP_WORDS.contains(&"er"));
        assert!(!DANISH_STOP_WORDS.contains(&"hest")); // "horse" - not a stop word
    }
}
