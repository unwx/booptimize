use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;

/// Tracks and ticks the progress bar with updated information.
pub async fn tick_progress(total_document_len: u64, mut progress_offset_receiver: Receiver<u64>) {
    let progress_bar = ProgressBar::new(total_document_len);
    let estimator = Arc::new(Mutex::new(Estimator::new(total_document_len, 0.2)));

    progress_bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/white}] {bytes}/{total_bytes} (ETA: {my_eta})",
        )
        .unwrap()
        .progress_chars("#>-")
        .with_key("my_eta", move |state: &ProgressState, w: &mut dyn Write| {
            let position = state.pos();
            let mut estimator = estimator.lock().unwrap();

            let eta = estimator.update(position);
            let formatted_eta = Estimator::format_eta(eta);

            write!(w, "{}", formatted_eta).unwrap();
        }),
    );

    loop {
        tokio::select! {
            offset = progress_offset_receiver.recv() => {
                 match offset {
                    Some(it) => progress_bar.set_position(it),
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(1000)) => {
                progress_bar.tick();
            }
        }
    }

    while let Some(offset) = progress_offset_receiver.recv().await {
        progress_bar.inc(offset);
    }

    progress_bar.finish_with_message("Done");
}

struct Estimator {
    total_len: u64,
    last_time: Option<Instant>,
    last_offset: u64,
    last_eta: Option<Duration>,
    smoothed_speed: f64,
    alpha: f64,
}

impl Estimator {
    pub fn new(total_len: u64, alpha: f64) -> Self {
        Self {
            total_len,
            last_time: None,
            last_offset: 0,
            last_eta: None,
            smoothed_speed: 0.0,
            alpha: alpha.clamp(0.0, 1.0),
        }
    }

    /// Update the estimator with new file offset update.
    pub fn update(&mut self, current_offset: u64) -> Option<Duration> {
        if current_offset == self.last_offset {
            return self.last_eta;
        }

        let now = Instant::now();

        if let Some(last_time) = self.last_time {
            let delta_time = now.duration_since(last_time).as_secs_f64();
            let delta_offset = current_offset.saturating_sub(self.last_offset) as f64;

            if delta_time > 0.001 {
                let current_speed = delta_offset / delta_time;

                if self.smoothed_speed == 0.0 {
                    self.smoothed_speed = current_speed;
                } else {
                    self.smoothed_speed =
                        (self.alpha * current_speed) + ((1.0 - self.alpha) * self.smoothed_speed);
                }
            }
        }

        self.last_time = Some(now);
        self.last_offset = current_offset;

        self.last_eta = {
            if self.smoothed_speed > 0.0 && current_offset < self.total_len {
                let remaining_bytes = self.total_len.saturating_sub(current_offset) as f64;
                let remaining_seconds = remaining_bytes / self.smoothed_speed;

                Some(Duration::from_secs_f64(remaining_seconds))
            } else {
                None
            }
        };

        self.last_eta
    }

    /// Helper method to format the duration into a readable HH:MM:SS string
    pub fn format_eta(eta: Option<Duration>) -> String {
        match eta {
            Some(duration) => {
                let secs = duration.as_secs();
                let hours = secs / 3600;
                let minutes = (secs % 3600) / 60;
                let seconds = secs % 60;

                if hours > 0 {
                    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
                } else {
                    format!("{:02}:{:02}", minutes, seconds)
                }
            }
            None => "??:??:??".to_string(),
        }
    }
}
