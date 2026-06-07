use crate::{LINE_ENDING, Section};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    sync::mpsc::{Receiver, Sender},
};

/// Write a series of sections to the destination.
pub async fn write<W>(
    destination: W,
    mut section_receiver: Receiver<Section>,
    progress_sender: Sender<u64>,
) where
    W: AsyncWrite + AsyncWriteExt + Unpin,
{
    let mut writer = BufWriter::new(destination);

    while let Some(section) = section_receiver.recv().await {
        writer
            .write_all(section.content.as_bytes())
            .await
            .expect("unable to write next document section");
        writer
            .write(LINE_ENDING.as_bytes())
            .await
            .expect("unable to write next document section new-line");
        writer
            .flush()
            .await
            .expect("unable to flush next optimized document section");

        progress_sender
            .send(section.file_offset)
            .await
            .expect("inter-thread progress channel is closed");
    }
}
