use crate::stream::StreamReader;

/// Audio Data Transport Stream (ADTS) used by MPEG TS or Shoutcast to stream audio.
/// https://wiki.multimedia.cx/index.php/ADTS
/// https://github.com/dholroyd/adts-reader/blob/master/src/lib.rs
/// https://docs.rs/symphonia-codec-aac/0.5.4/src/symphonia_codec_aac/adts.rs.html

#[derive(Debug)]
pub enum AdtsHeaderError {
    /// Indicates that the given buffer did not start with the required sequence of 12 '1'-bits
    /// (`0xfff`).
    BadSyncWord,
    /// Length of header is different than expected
    InvalidLength
}

pub struct AdtsHeader {
    header_len: usize,
    buf: Vec<u8>,
}

impl AdtsHeader {
    const SIZE: usize = 7;

    pub async fn read(stream: &'_ mut dyn StreamReader) -> Result<Self, AdtsError> {
        let mut s    = Self { buf: vec![0; Self::SIZE], header_len: Self::SIZE };
        if stream.async_read(&mut s.buf).await.is_err() {
            return Err(AdtsError::IoError);
        }

        if s.sync_word() != 0xfff {
            return Err(AdtsError::Header(AdtsHeaderError::BadSyncWord));
        }
        // Skipping MPEG version bit
        // Skipping layer version 2 bits
        if s.crc_protection_present() {
            s.header_len += 2;
            s.buf.extend_from_slice(&[0, 0]);
            if stream.async_read(&mut s.buf[7..9]).await.is_err() {
                return Err(AdtsError::IoError);
            }
        }
        // Going directly to frame length as it's crucial for it to be valid
        // before processing other fields
        if s.frame_length() < s.header_len as u16 {
            return Err(AdtsError::Header(AdtsHeaderError::InvalidLength));
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

#[derive(Debug)]
pub enum AdtsError {
    Header(AdtsHeaderError),
    InvalidFrameLength,
    /// IO error
    IoError
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

    pub async fn read(&mut self) -> Result<Vec<u8>, AdtsError> {
        // TODO: Support multiple AAC packets per ADTS packet.
        // https://docs.rs/symphonia-codec-aac/0.5.4/src/symphonia_codec_aac/adts.rs.html#30
        // https://wiki.multimedia.cx/index.php/ADTS recommends to only have one AAC packet per one
        // ADTS packet
        let mut header = match AdtsHeader::read(self.stream).await {
            Err(e) => return Err(e),
            Ok(v) => v
        };

        let frame_len = header.frame_length() as usize;
        if frame_len < header.header_len {
            return Err(AdtsError::InvalidFrameLength);
        }

        // TODO: More efficient way for allocation here
        let mut ret = vec![0; frame_len - header.header_len];
        if self.stream.async_read(&mut ret).await.is_err() {
            return Err(AdtsError::IoError);
        }

        header.buf.append(&mut ret);
        Ok(header.buf)
    }
}
