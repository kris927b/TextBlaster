// Utils

pub mod common;
pub mod prometheus_metrics;
pub mod text; // Added to declare the common.rs module

pub use text::{
    find_all_duplicate, find_duplicates, find_top_duplicate, get_n_grams, split_into_sentences,
    split_into_words, PUNCTUATION,
};
