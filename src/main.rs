use std::path::MAIN_SEPARATOR_STR;

use crate::util::SectionReader;
use clap::Parser;
use ollama_rs::{
    Ollama,
    generation::chat::{ChatMessage, MessageRole, request::ChatMessageRequest},
    models::ModelOptions,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc::{self, Receiver, Sender},
};

mod util;

const INSTRUCTION: &str =
     "You will be given an academic page section.
     Your goal is to remove everything @uneccessary.

     RULES:
     1. You never append, amend, or modify the content; you can only remove @uneccessary sentences.
     2. In other words, you must preserve the original content, and only remove the @uneccessary noise.
     3. You must preserve the original Markdown formatting and references, and must not alter it in any way.
     4. You may remove non-markdown formatting, such as <span>.

     WHAT IS @uneccessary
     1. Meta-text and Book Mechanics: References to other chapters or previous sections (e.g., \"So far we have...\", \"We will discuss... in Chapter X\").
     2. Conversational Filler: Subjective observations, industry commentary, or transitions that lack core technical facts (e.g., \"This receives a lot of commercial interest\").
     3. Further Reading Pointers: Sentences whose sole purpose is directing the reader to external sources.
     4. Non-Markdown Formatting: Stray HTML tags that clutter the text.";


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Document to optimize/compress.
    #[arg(index = 1)]
    in_doc: String,

    /// Resulting document.
    #[arg(index = 2)]
    out_doc: String,

    /// The Ollama model to use.
    #[arg(short = 'm', long)]
    model: String,
}

#[tokio::main]
async fn main() {
    let args = Args::try_parse().expect("invalid arguments");
    println!("Original document: {}", &args.in_doc);
    println!("Optimized document: {}", &args.out_doc);
    println!("Using Ollama model: {}", &args.model);

    if !args.in_doc.ends_with(".md") {
        panic!("only Markdown files are supported at the moment");
    }

    let in_doc = File::open(&args.in_doc).await.unwrap_or_else(|e| {
        panic!(
            "unable to open original document for reading '{}': {}",
            args.in_doc, e
        )
    });
    let out_doc = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.out_doc)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "unable to open optimized document for writing '{}': {}",
                args.out_doc, e
            )
        });

    let (raw_section_sender, raw_section_receiver) = mpsc::channel(1);
    let (optimized_section_sender, optimized_section_receiver) = mpsc::channel(3);

    let read_handle = tokio::spawn(read(in_doc, raw_section_sender));
    let optimize_handle = tokio::spawn(optimize(
        args.model,
        raw_section_receiver,
        optimized_section_sender,
    ));
    let write_handle = tokio::spawn(write(out_doc, optimized_section_receiver));

    let _ = tokio::join!(read_handle, optimize_handle, write_handle);
}

async fn read(file: File, raw_section_sender: Sender<String>) {
    let file_size = file
        .metadata()
        .await
        .expect("unable to get document size")
        .len();

    let mut reader = SectionReader::new(BufReader::new(file));

    loop {
        let section = reader
            .next()
            .await
            .expect("unable to read next document section")
            .to_string();

        if section.is_empty() {
            return;
        }

        raw_section_sender
            .send(section)
            .await
            .expect("document raw_section channel is closed");

        let current_pos = reader.stream_position().await.unwrap_or(0);
        println!(
            "({:.2}%): {}/{}",
            (current_pos as f64 / file_size as f64) * 100.0,
            current_pos,
            file_size
        );
    }
}

async fn optimize(
    model: String,
    mut raw_section_receiver: Receiver<String>,
    optimized_section_sender: Sender<String>,
) {
    let ollama = Ollama::default();

    while let Some(section) = raw_section_receiver.recv().await {
        let messages = vec![
            ChatMessage::new(MessageRole::System, INSTRUCTION.to_string()),
            ChatMessage::new(MessageRole::User, section),
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
            .expect("unable to make Ollama request to optimize the section");

        optimized_section_sender
            .send(response)
            .await
            .expect("document optimized_section channel is closed");
    }
}

async fn write(file: File, mut optimized_section_receiver: Receiver<String>) {
    let mut writer = BufWriter::new(file);

    while let Some(section) = optimized_section_receiver.recv().await {
        writer
            .write_all(section.as_bytes())
            .await
            .expect("unable to write next optimized document section");
        writer
            .write(MAIN_SEPARATOR_STR.as_bytes())
            .await
            .expect("unable to write next optimized document section new-line");
        writer
            .flush()
            .await
            .expect("unable to flush next optimized document section");
    }

    println!("Done");
}
