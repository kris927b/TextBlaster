use serde::Deserialize;

// Keep existing config
#[derive(Deserialize, Debug, Clone)]
pub struct ParquetInputConfig {
    pub path: String,              // Path to the Parquet file or directory
    pub text_column: String,       // Name of the column containing the main text
    pub id_column: Option<String>, // Optional: Name of a column to use as document ID
    // Add other column mappings as needed (e.g., for metadata)
    pub batch_size: Option<usize>, // Optional: Arrow batch size for reading
}
