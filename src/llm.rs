use crate::Section;
use ollama_rs::{
    Ollama,
    generation::chat::{ChatMessage, MessageRole, request::ChatMessageRequest},
    models::ModelOptions,
};
use tokio::sync::mpsc::{Receiver, Sender};

/// Transformed the sections using the specified `model` and its parameters.
pub async fn transform(
    model: String,
    instruction: String,
    mut raw_section_receiver: Receiver<Section>,
    transformed_section_sender: Sender<Section>,
) {
    let ollama = Ollama::default();

    while let Some(section) = raw_section_receiver.recv().await {
        let messages = vec![
            ChatMessage::new(MessageRole::System, instruction.clone()),
            ChatMessage::new(MessageRole::User, section.content),
        ];

        let request = ChatMessageRequest::new(model.clone(), messages).options(
            ModelOptions::default()
                .temperature(0.0)
                .seed(0)
                .mirostat(0)
                .num_ctx(8192)
                .num_predict(-1)
                .repeat_penalty(1.00),
        );
        let response = ollama
            .send_chat_messages(request)
            .await
            .map(|it| it.message.content)
            .expect("unable to make Ollama request to transform the section");

        transformed_section_sender
            .send(Section::new(response, section.file_offset))
            .await
            .expect("inter-thread tranformed_section channel is closed");
    }
}
