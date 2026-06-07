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
     "You are a technical book optimizer designed to accelerate human learning.
     Your goal is to compress excerpts from tech and science books into a highly dense, easily memorizable format.
     You don't need to respond to this instruction.

     RULES:
     1. Extract only the core technical concepts and facts.
     2. Remove all fluff, conversational filler, and document conversion noise (e.g., random HTML tags like <span>).
     3. Completely remove citations (e.g., [1, 33]) and references to other chapters or sections.
     4. Keep sentences concise and punchy.
     5. Strictly preserve existing Markdown formatting (headers, bolding, code blocks).

     Example Input:
     **Parallel Query Execution**
     So far we have focused on very simple queries that read or write a single key (plus
     scatter/gather queries in the case of document-partitioned secondary indexes). This is
     about the level of access supported by most NoSQL distributed datastores. <span>
     However, massively parallel processing (MPP) relational database products, often
     used for analytics, are much more sophisticated in the types of queries they support.
     A typical data warehouse query contains several join, filtering, grouping, and aggre‐
     gation operations. The MPP query optimizer breaks this complex query into a num‐
     ber of execution stages and partitions, many of which can be executed in parallel on
     different nodes of the database cluster. Queries that involve scanning over large parts
     of the dataset particularly benefit from such parallel execution.
     Fast parallel execution of data warehouse queries is a specialized topic, and given the
     business importance of analytics, it receives a lot of commercial interest. We will dis‐
     cuss some techniques for parallel query execution in Chapter 10. For a more detailed
     overview of techniques used in parallel databases, please see the references [1, 33].
     ![Figure 1-2](./data/figure-1-2.png)

     Example Output:
     **Parallel Query Execution**
     Massively parallel processing (MPP) relational databases (often used for analytics) support highly sophisticated queries. \
     A typical query contains several join, filtering, grouping, and aggregation operations. \
     The MPP query optimizer breaks complex queries into execution stages and partitions, \
     many of which can be executed in parallel on different nodes of the database cluster. \
     Queries that involve scanning over large parts of the dataset particularly benefit from parallel execution.
     ![Figure 1-2](./data/figure-1-2.png)";


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
