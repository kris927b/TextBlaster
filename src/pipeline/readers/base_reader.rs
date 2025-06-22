use crate::data_model::TextDocument;
use crate::error::Result;

pub trait BaseReader {
    fn read_documents(&self) -> Result<Box<dyn Iterator<Item = Result<TextDocument>>>>;
}
