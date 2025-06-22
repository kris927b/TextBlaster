use crate::data_model::TextDocument;
use crate::error::Result;

/// Trait for writing batches of TextDocuments to an output sink (e.g. file).
pub trait BaseWriter {
    /// Write a batch of documents to the sink.
    fn write_batch(&mut self, documents: &[TextDocument]) -> Result<()>;

    /// Finalize and close the output writer.
    fn close(self) -> Result<()>;
}
