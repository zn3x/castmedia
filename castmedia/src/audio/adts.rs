use anyhow::Result;

use crate::{audio::AudioReader, stream::StreamReader};

/// Audio Data Transport Stream (ADTS) used by MPEG TS or Shoutcast to stream audio.
/// https://wiki.multimedia.cx/index.php/ADTS
/// https://github.com/dholroyd/adts-reader/blob/master/src/lib.rs
/// https://docs.rs/symphonia-codec-aac/0.5.4/src/symphonia_codec_aac/adts.rs.html

pub struct AdtsHeader {
    header_len: usize,
    buf: Vec<u8>,
}

impl AdtsHeader {
    const SIZE: usize = 7;

    pub async fn read(stream: &'_ mut dyn StreamReader) -> Result<Self> {
        let mut s    = Self { buf: vec![0; Self::SIZE], header_len: Self::SIZE };
        if stream.async_read(&mut s.buf).await.is_err() {
            return Err(anyhow::Error::msg("ADTS IO error"));
        }

        if s.sync_word() != 0xfff {
            // The given buffer did not start with the required sequence of 12 '1'-bits
            // (`0xfff`).
            return Err(anyhow::Error::msg("ADTS Bad sync word"));
        }
        // Skipping MPEG version bit
        // Skipping layer version 2 bits
        if s.crc_protection_present() {
            s.header_len += 2;
            s.buf.extend_from_slice(&[0, 0]);
            if stream.async_read(&mut s.buf[7..9]).await.is_err() {
                return Err(anyhow::Error::msg("ADTS IO error"));
            }
        }
        // Going directly to frame length as it's crucial for it to be valid
        // before processing other fields
        if s.frame_length() < s.header_len as u16 {
            return Err(anyhow::Error::msg("ADTS Length of header is different than expected"));
        }

        Ok(s)
    }

    /// Return the sync word at the start the adts container
    fn sync_word(&self) -> u16 {
        u16::from(self.buf[0]) << 4 | u16::from(self.buf[1] >> 4)
    }

    /// Check whether protection bit is present (0) or is absent (1)
    fn crc_protection_present(&self) -> bool {
        self.buf[1] & 0b0000_0001 == 0
    }

    /// length of this frame, including the length of the header.
    pub fn frame_length(&self) -> u16 {
        u16::from(self.buf[3] & 0b11) << 11
            | u16::from(self.buf[4]) << 3
            | u16::from(self.buf[5]) >> 5
    }

}

pub struct AdtsReader<'a> {
    stream: &'a mut dyn StreamReader,
}

impl<'a> AdtsReader<'a> {
    pub fn new(stream: &'a mut dyn StreamReader) -> Self {
        Self {
            stream,
        }
    }
}

#[async_trait::async_trait]
impl AudioReader for AdtsReader<'_> {
    async fn read(&mut self) -> Result<Vec<u8>> {
        // TODO: Support multiple AAC packets per ADTS packet.
        // https://docs.rs/symphonia-codec-aac/0.5.4/src/symphonia_codec_aac/adts.rs.html#30
        // https://wiki.multimedia.cx/index.php/ADTS recommends to only have one AAC packet per one
        // ADTS packet
        let mut header = AdtsHeader::read(self.stream).await?;
        let frame_len  = header.frame_length() as usize;
        if frame_len < header.header_len {
            return Err(anyhow::Error::msg("ADTS Invalid frame length"));
        }

        // TODO: More efficient way for allocation here
        let mut ret = vec![0; frame_len - header.header_len];
        if self.stream.async_read(&mut ret).await.is_err() {
            return Err(anyhow::Error::msg("ADTS IO error"));
        }

        header.buf.append(&mut ret);
        Ok(header.buf)
    }
}
