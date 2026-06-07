use std::time::Duration;

use crate::util::SectionReader;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
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

const LINE_ENDING: &str = if cfg!(target_family = "windows") {
    "\r\n"
} else {
    "\n"
};

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

    /// File containing the instructions for the model.
    #[arg(short = 'i', long)]
    instruction_file: String,
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

    let instruction = tokio::fs::read_to_string(&args.instruction_file)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "unable to open instruction file for reading '{}': {}",
                args.instruction_file, e
            )
        });
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
    let in_doc_size = in_doc
        .metadata()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "unable to get original document filesize '{}': {}",
                args.in_doc, e
            )
        })
        .len();

    let (raw_section_sender, raw_section_receiver) = mpsc::channel(1);
    let (optimized_section_sender, optimized_section_receiver) = mpsc::channel(3);
    let (progress_sender, progress_receiver) = mpsc::channel(1);

    let read_handle = tokio::spawn(read(in_doc, raw_section_sender));
    let optimize_handle = tokio::spawn(optimize(
        args.model,
        instruction,
        raw_section_receiver,
        optimized_section_sender,
    ));
    let write_handle = tokio::spawn(write(out_doc, optimized_section_receiver, progress_sender));
    let notify_handle = tokio::spawn(notify_progress(in_doc_size, progress_receiver));

    let _ = tokio::join!(read_handle, optimize_handle, write_handle, notify_handle);
}

async fn read(file: File, raw_section_sender: Sender<Section>) {
    let mut reader = SectionReader::new(BufReader::new(file));

    loop {
        let section_content = reader
            .next()
            .await
            .expect("unable to read next document section")
            .to_string();
        let file_offset = reader.stream_position().await.unwrap_or(0) as u64;

        if section_content.is_empty() {
            return;
        }

        raw_section_sender
            .send(Section::new(section_content, file_offset))
            .await
            .expect("document raw_section channel is closed");
    }
}

async fn optimize(
    model: String,
    instruction: String,
    mut raw_section_receiver: Receiver<Section>,
    optimized_section_sender: Sender<Section>,
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
            .expect("unable to make Ollama request to optimize the section");

        optimized_section_sender
            .send(Section::new(response, section.file_offset))
            .await
            .expect("document optimized_section channel is closed");
    }
}

async fn write(
    file: File,
    mut optimized_section_receiver: Receiver<Section>,
    progress_sender: Sender<u64>,
) {
    let mut writer = BufWriter::new(file);

    while let Some(section) = optimized_section_receiver.recv().await {
        writer
            .write_all(section.content.as_bytes())
            .await
            .expect("unable to write next optimized document section");
        writer
            .write(LINE_ENDING.as_bytes())
            .await
            .expect("unable to write next optimized document section new-line");
        writer
            .flush()
            .await
            .expect("unable to flush next optimized document section");

        progress_sender
            .send(section.file_offset)
            .await
            .expect("progress_bar channel is closed");
    }

    println!("Done");
}

async fn notify_progress(file_len: u64, mut progress_offset_receiver: Receiver<u64>) {
    let progress_bar = ProgressBar::new(file_len);
    progress_bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/white}] {bytes}/{total_bytes} (ETA: {eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    loop {
        tokio::select! {
            offset = progress_offset_receiver.recv() => {
                 match offset {
                    Some(it) => progress_bar.set_position(it),
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                progress_bar.tick();
            }
        }
    }

    while let Some(offset) = progress_offset_receiver.recv().await {
        progress_bar.inc(offset);
    }

    progress_bar.finish_with_message("Done");
}


struct Section {
    content: String,
    file_offset: u64,
}

impl Section {
    pub fn new(content: String, file_offset: u64) -> Self {
        Self {
            content,
            file_offset,
        }
    }
}
