pub mod openai;
pub mod anthropic;
pub mod ollama;
pub mod gemini;
pub mod factory;

use async_trait::async_trait;
use blockcell_core::types::{ChatMessage, LLMResponse, StreamEvent};
use blockcell_core::Result;
use serde_json::Value;
use tokio::sync::mpsc;

#[async_trait]
pub trait Provider: Send + Sync {
    async fn chat(&self, messages: &[ChatMessage], tools: &[Value]) -> Result<LLMResponse>;

    /// Streaming variant of `chat`. Sends `StreamEvent` chunks through `tx`.
    /// The final `StreamEvent::Done` carries the aggregated `LLMResponse`.
    ///
    /// Default implementation falls back to non-streaming `chat` and emits
    /// the full response as a single `ContentDelta` + `Done`.
    async fn chat_stream(
        &self,
        messages: &[ChatMessage],
        tools: &[Value],
        tx: mpsc::UnboundedSender<StreamEvent>,
    ) -> Result<LLMResponse> {
        let response = self.chat(messages, tools).await?;
        if let Some(content) = &response.content {
            let _ = tx.send(StreamEvent::ContentDelta(content.clone()));
        }
        let _ = tx.send(StreamEvent::Done(response.clone()));
        Ok(response)
    }
}

pub use openai::OpenAIProvider;
pub use anthropic::AnthropicProvider;
pub use ollama::OllamaProvider;
pub use gemini::GeminiProvider;
pub use factory::{create_provider, create_main_provider, create_evolution_provider, infer_provider_from_model};
