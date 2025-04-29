// Text utils
use itertools::Itertools;

use icu::segmenter::{SentenceSegmenter, WordSegmenter};
use once_cell::sync::Lazy;
use std::collections::HashSet;

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
    let segmenter = SentenceSegmenter::new(); // Assuming Danish - adjust locale as needed

    segmenter
        .segment_str(text)
        .tuple_windows()
        .map(|(i, j)| &text[i..j])
        .collect()
}

pub fn split_into_words(text: &str) -> Vec<&str> {
    let segmenter = WordSegmenter::new_auto();

    segmenter
        .segment_str(text)
        .iter_with_word_type()
        .tuple_windows()
        .filter(|(_, (_, segment_type))| segment_type.is_word_like())
        .map(|((i, _), (j, _))| &text[i..j])
        .collect()
}
