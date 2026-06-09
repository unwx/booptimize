use crate::{llm::transform, reader::read, user::tick_progress, writer::write};
use clap::Parser;
use regex::Regex;
use tokio::{
    fs::{File, OpenOptions},
    sync::mpsc::{self},
};

mod llm;
mod reader;
mod user;
mod writer;

const LINE_ENDING: &str = if cfg!(target_family = "windows") {
    "\r\n"
} else {
    "\n"
};

#[derive(Debug)]
struct Section {
    /// Single paragraph
    content: String,

    /// Offset in bytes from the start of the file where this section begins.
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

    /// The size of the context window used to generate the next token.
    #[arg(long, default_value_t = 8092)]
    context_window: u64,

    /// Regex pattern of a string to find, which should be used as initial point of the optimization.
    #[arg(long, default_value = "")]
    resume_from: String,
}

#[tokio::main]
async fn main() {
    let args = Args::try_parse().expect("invalid arguments");
    println!("Original document: {}", &args.in_doc);
    println!("Optimized document: {}", &args.out_doc);
    println!("Ollama model: {}", &args.model);
    println!("Ollama instruction file: {}", &args.instruction_file);
    println!("Ollama context window: {}", &args.context_window);

    if !args.resume_from.is_empty() {
        println!("Resume from regex pattern: {}", &args.resume_from);
    }

    let (instruction, in_doc, out_doc, in_doc_meta) = tokio::try_join!(
        async {
            tokio::fs::read_to_string(&args.instruction_file)
                .await
                .map_err(|e| {
                    format!(
                        "unable to open instruction file for reading '{}': {}",
                        args.instruction_file, e
                    )
                })
        },
        async {
            File::open(&args.in_doc).await.map_err(|e| {
                format!(
                    "unable to open original document for reading '{}': {}",
                    args.in_doc, e
                )
            })
        },
        async {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&args.out_doc)
                .await
                .map_err(|e| {
                    format!(
                        "unable to open optimized document for writing '{}': {}",
                        args.out_doc, e
                    )
                })
        },
        async {
            tokio::fs::metadata(&args.in_doc).await.map_err(|e| {
                format!(
                    "unable to get original document filesize '{}': {}",
                    args.in_doc, e
                )
            })
        },
    )
    .unwrap_or_else(|err_msg| {
        panic!("{}", err_msg);
    });
    let resume_from = {
        if !args.resume_from.is_empty() {
            Some(Regex::new(&args.resume_from).expect("invalid resume_from regex pattern"))
        } else {
            None
        }
    };

    let (raw_section_sender, raw_section_receiver) = mpsc::channel(1);
    let (transformed_section_sender, transformed_section_receiver) = mpsc::channel(2);
    let (progress_sender, progress_receiver) = mpsc::channel(1);

    let read_handle = tokio::spawn(read(in_doc, resume_from, raw_section_sender));
    let optimize_handle = tokio::spawn(transform(
        args.model,
        instruction,
        args.context_window,
        raw_section_receiver,
        transformed_section_sender,
    ));
    let write_handle = tokio::spawn(write(
        out_doc,
        transformed_section_receiver,
        progress_sender,
    ));
    let progress_handle = tokio::spawn(tick_progress(in_doc_meta.len(), progress_receiver));

    let _ = tokio::join!(read_handle, optimize_handle, write_handle, progress_handle);
}
