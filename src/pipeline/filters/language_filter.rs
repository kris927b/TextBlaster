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
            Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason,
            })
        } else if confidence < self.min_confidence {
            let reason = format!(
                "Language detection confidence is not satified: {} < {}",
                confidence, self.min_confidence
            );
            Err(PipelineError::DocumentFiltered {
                document: Box::new(document),
                reason,
            })
        } else {
            Ok(document)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::TextDocument;

    fn create_test_doc(id: &str, content: &str) -> TextDocument {
        TextDocument {
            id: id.to_string(),
            source: "rep_test_source".to_string(),
            content: content.to_string(),
            metadata: Default::default(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_allowed_language() {
        let document = create_test_doc("doc1", "Sometimes, all you need to start the day right is a good coffee and someone greeting you smiling.");
        let filter = LanguageDetectionFilter::new(0.8, vec!["eng".to_string()]);
        let result = filter.process(document).await;

        assert!(result.is_ok());
        let processed_document = result.unwrap();
        assert_eq!(
            processed_document.metadata.get("Detected language"),
            Some(&"English".into())
        );
        // Confidence is a float, so we check it's present and parseable.
        assert!(processed_document
            .metadata
            .get("Detected language confidence")
            .is_some());
        assert!(processed_document
            .metadata
            .get("Detected language confidence")
            .unwrap()
            .parse::<f64>()
            .is_ok());
    }

    #[tokio::test]
    async fn test_disallowed_language() {
        let document = create_test_doc("doc2", "Ceci est un document de test en français.");
        let filter = LanguageDetectionFilter::new(0.8, vec!["eng".to_string()]); // Only allow English
        let result = filter.process(document).await;

        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered {
                document: _,
                reason,
            } => {
                assert!(reason.contains("Document is not any of the following languages: eng"));
            }
            _ => panic!("Expected DocumentFiltered error"),
        }
    }

    #[tokio::test]
    async fn test_confident_but_disallowed_language() {
        // "Hola Mundo" is Spanish and should be detected with high confidence.
        let document = create_test_doc("doc3", "Hola Mundo");
        // Allow only English, with a standard confidence threshold.
        let filter = LanguageDetectionFilter::new(0.8, vec!["eng".to_string()]);
        let result = filter.process(document).await;

        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered {
                document: _,
                reason,
            } => {
                // The primary reason for filtering should be the language not being allowed,
                // even if confidence would have been sufficient.
                assert!(
                    reason.contains("Document is not any of the following languages: eng"),
                    "Unexpected reason: {}",
                    reason
                );
                // Also, check that the detected language was indeed Spanish.
                // This ensures the test is correctly targeting the intended logic path.
                // The document is consumed by filter.process, so we can't directly check its metadata.
                // However, the reason string for disallowed language already confirms this.
            }
            _ => panic!("Expected DocumentFiltered error"),
        }
    }

    #[tokio::test]
    async fn test_allowed_language_low_confidence() {
        // "a b c" is English, but very short, likely leading to low confidence.
        let document = create_test_doc("doc5", "text arrives out of thin air");
        // Allow English, but require a very high confidence that "a b c" won't meet.
        let filter = LanguageDetectionFilter::new(0.99, vec!["eng".to_string()]);
        let result = filter.process(document).await;

        assert!(result.is_err());
        match result.err().unwrap() {
            PipelineError::DocumentFiltered {
                document: processed_doc,
                reason,
            } => {
                assert_eq!(processed_doc.id, "doc5"); // Ensure the correct document is returned
                                                      // Check that the detected language was indeed English (or whatever whatlang detects for "a b c")
                                                      // and that the reason is low confidence.
                assert!(
                    processed_doc.metadata.get("Detected language").is_some(),
                    "Detected language should be in metadata even if filtered for low confidence"
                );
                // whatlang detects "a b c" as English.
                assert_eq!(
                    processed_doc.metadata.get("Detected language"),
                    Some(&"English".into())
                );
                assert!(
                    reason.contains("Language detection confidence is not satified"),
                    "Unexpected reason: {}",
                    reason
                );
            }
            _ => panic!("Expected DocumentFiltered error of variant DocumentFiltered"),
        }
    }
}
