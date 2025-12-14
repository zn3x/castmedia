use anyhow::Result;

use crate::{audio::AudioReader, stream::StreamReader};

pub struct Mp3Reader<'a> {
    stream: &'a mut dyn StreamReader,
    buf: Vec<u8>
}

impl<'a> Mp3Reader<'a> {
    const BUFFER_SIZE: usize = 1024;

    pub fn new(stream: &'a mut dyn StreamReader) -> Self {
        Self {
            stream,
            buf: vec![ 0u8; Self::BUFFER_SIZE ]
        }
    }
}

#[async_trait::async_trait]
impl AudioReader for Mp3Reader<'_> {
    async fn read(&mut self) -> Result<Vec<u8>> {

        match self.stream.async_read(&mut self.buf).await {
            Ok(r) => Ok(self.buf[..r].to_vec()),
            Err(_) => Err(anyhow::Error::msg("MP3 IO error"))
        }
    }
}
