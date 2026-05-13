use anyhow::Result;

mod adts;
mod mp3;

pub use adts::AdtsReader;
pub use mp3::Mp3Reader;

#[async_trait::async_trait]
pub trait AudioReader {
    /// Read one complete codec frame from the stream.
    ///
    /// This method is cancel-safe: if the returned future is dropped at an
    /// `.await` point (e.g., by `tokio::select!`), all progress is preserved
    /// in `self.state`. The next call to `read()` will resume from exactly
    /// where it left off, no bytes from the stream are lost.
    async fn read(&mut self) -> Result<Vec<u8>>;
}
