use crate::Section;
use regex::Regex;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncRead, AsyncSeekExt, BufReader},
    sync::mpsc::Sender,
};

/// Reads the entire content from `source`, section-by-section, and sends with `raw_section_sender`.
pub async fn read<R>(source: R, mut resume_from: Option<Regex>, raw_section_sender: Sender<Section>)
where
    R: AsyncRead + AsyncSeekExt + Unpin,
{
    let mut reader = SectionReader::new(BufReader::new(source));

    loop {
        let section_content = reader
            .next()
            .await
            .expect("unable to read the next document section");

        if section_content.is_empty() {
            return;
        }

        if let Some(pattern) = resume_from.as_ref() {
            if !pattern.is_match(&section_content) {
                continue;
            }

            resume_from = None;
        }

        raw_section_sender
            .send(Section::new(
                section_content.to_string(),
                reader.stream_position().await.unwrap_or(0) as u64,
            ))
            .await
            .expect("inter-thread raw_section channel is closed");
    }
}


/// Markdown file section reader.
///
/// Section, in this context, is anything that belongs to the same Markdown header (like div in HTML).
pub struct SectionReader<R> {
    /// Internal reader.
    reader: BufReader<R>,

    /// Section buffer.
    buf: String,

    /// Section length, always ` <= ` than `buf`,
    /// as the buffer may contain multiple sections.
    len: usize,
}

impl<R: AsyncRead + Unpin> SectionReader<R> {
    pub fn new(reader: BufReader<R>) -> Self {
        Self {
            reader,
            buf: "".into(),
            len: 0,
        }
    }

    /// Returns a reference to the next section.
    ///
    /// Reference will be empty on EOF.
    pub async fn next(&mut self) -> io::Result<&str> {
        self.buf.drain(..self.len);
        self.len = self.buf.len();

        while self.reader.read_line(&mut self.buf).await? != 0 {
            if self.buf.len() == self.len {
                break;
            }

            let is_header = {
                let line = &self.buf[self.len..];

                if line.starts_with('#') {
                    line.starts_with("# ")
                        || line.starts_with("## ")
                        || line.starts_with("### ")
                        || line.starts_with("#### ")
                        || line.starts_with("##### ")
                        || line.starts_with("###### ")
                } else {
                    false
                }
            };

            if is_header {
                if self.len != 0 {
                    // We already have something,
                    // so we must be inside a section.
                    return Ok(&self.buf[..self.len]);
                }
            }

            self.len = self.buf.len();
        }

        if !self.buf.is_empty() {
            return Ok(&self.buf[..]);
        }

        Ok(&self.buf[0..0])
    }
}

impl<R> SectionReader<R>
where
    R: AsyncRead + AsyncSeekExt + Unpin,
{
    /// Returns current progress of the internal reader.
    pub async fn stream_position(&mut self) -> io::Result<usize> {
        self.reader.stream_position().await.map(|it| it as usize)
    }
}
