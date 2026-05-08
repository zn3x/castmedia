use anyhow::Result;

mod adts;
mod mp3;

pub use adts::AdtsReader;
pub use mp3::Mp3Reader;

#[async_trait::async_trait]
pub trait AudioReader {
    async fn read(&mut self) -> Result<Vec<u8>>;
}
