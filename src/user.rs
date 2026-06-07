use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::mpsc::Receiver;

/// Tracks and ticks the progress bar with updated information.
pub async fn tick_progress(total_document_len: u64, mut progress_offset_receiver: Receiver<u64>) {
    let progress_bar = ProgressBar::new(total_document_len);
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
