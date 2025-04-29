// src/pipeline/filters/mod.rs

mod c4_filters; // Looks for src/pipeline/filters/C4_filter.rs
mod gopher_quality;
mod gopher_rep;

// Re-export the main type
pub use c4_filters::C4QualityFilter; // Assuming C4Filter struct/enum exists
pub use gopher_quality::GopherQualityFilter;
pub use gopher_rep::GopherRepetitionFilter;
