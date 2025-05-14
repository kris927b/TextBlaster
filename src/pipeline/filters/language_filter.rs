use async_trait::async_trait;
use whatlang::{detect, Lang};

use crate::data_model::TextDocument;
use crate::error::{PipelineError, Result};
use crate::executor::ProcessingStep;

pub struct LanguageDetectionFilter {
    min_confidence: f64,
    allowed_languages: Vec<String>,
}

impl LanguageDetectionFilter {
    pub fn new(min_confidence: f64, allowed_languages: Vec<String>) -> Self {
        LanguageDetectionFilter {
            min_confidence,
            allowed_languages,
        }
    }
}

#[async_trait]
impl ProcessingStep for LanguageDetectionFilter {
    fn name(&self) -> &'static str {
        "LanguageDetectionFilter"
    }

    async fn process(&self, document: TextDocument) -> Result<TextDocument> {
        let mut document = document;
        let text = &document.content;

        let lang_detect = detect(text).unwrap();
        let lang: Lang = lang_detect.lang();
        let confidence = lang_detect.confidence();

        document
            .metadata
            .insert("Detected language".into(), lang.name().into());
        document.metadata.insert(
            "Detected language confidence".into(),
            confidence.to_string(),
        );

        if !self.allowed_languages.contains(&lang.code().into()) {
            let reason = format!(
                "Document is not any of the following languages: {}",
                self.allowed_languages.join("; ")
            );
            Err(PipelineError::DocumentFiltered { document, reason })
        } else if confidence < self.min_confidence {
            let reason = format!(
                "Language detection confidence is not satified: {} < {}",
                confidence, self.min_confidence
            );
            Err(PipelineError::DocumentFiltered { document, reason })
        } else {
            Ok(document)
        }
    }
}
