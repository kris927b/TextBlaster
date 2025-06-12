// Utils

pub mod prometheus_metrics;
pub mod text;
pub mod utils; // Added to declare the utils.rs module // Added to declare the prometheus_metrics.rs module

pub use text::{
    find_all_duplicate, find_duplicates, find_top_duplicate, get_n_grams, split_into_sentences,
    split_into_words, PUNCTUATION,
};
