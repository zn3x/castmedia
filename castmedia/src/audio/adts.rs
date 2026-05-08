use anyhow::Result;

use crate::{audio::AudioReader, stream::StreamReader};

/// Audio Data Transport Stream (ADTS) used by MPEG TS or Shoutcast to stream audio.
/// https://wiki.multimedia.cx/index.php/ADTS
/// https://github.com/dholroyd/adts-reader/blob/master/src/lib.rs
/// https://docs.rs/symphonia-codec-aac/0.5.4/src/symphonia_codec_aac/adts.rs.html
///
/// ADTS header bit layout:
/// ```text
/// AAAAAAAA AAAABCCD EEFFFFGH HHIJKLMM MMMMMMMM MMMOOOOO OOOOOOPP (QQQQQQQQ QQQQQQQQ)
/// ```
///
/// For broadcast, we only care about the fields needed to correctly delimit frames
/// and detect corrupt data. The listeners' AAC decoders handle everything else.
///
/// | Letters | Bits | Used? | Why                                       |
/// |---------|------|-------|-------------------------------------------|
/// | A       | 12   | YES   | Syncword — required to find frame start   |
/// | B       | 1    | no    | MPEG version — decoder's job              |
/// | C       | 2    | YES   | Layer — must be 0, validates frame        |
/// | D       | 1    | YES   | Protection absent — determines header size|
/// | E       | 2    | no    | Profile — decoder's job                   |
/// | F       | 4    | YES   | Sampling freq index — 15 is forbidden     |
/// | G       | 1    | no    | Private bit — informational only           |
/// | H       | 3    | no    | Channel config — decoder's job            |
/// | I–L     | 4    | no    | Originality/Home/Copyright — info only    |
/// | M       | 13   | YES   | Frame length — required to read the frame |
/// | O       | 11   | no    | Buffer fullness — informational only       |
/// | P       | 2    | YES   | Num AAC frames — must read complete frame  |
/// | Q       | 16   | no    | CRC — present/absent affects header size  |

#[derive(Debug)]
pub struct AdtsHeader {
    header_len: usize,
    buf: Vec<u8>,
}

impl AdtsHeader {
    const SIZE: usize = 7;

    pub async fn read(stream: &'_ mut dyn StreamReader) -> Result<Self> {
        let mut s = Self { buf: vec![0; Self::SIZE], header_len: Self::SIZE };
        read_exact(stream, &mut s.buf).await?;

        if s.sync_word() != 0xfff {
            return Err(anyhow::Error::msg("ADTS Bad sync word"));
        }

        if s.layer() != 0 {
            return Err(anyhow::Error::msg("ADTS Invalid layer"));
        }

        let sf_index = s.sampling_frequency_index();
        // Indices 13 and 14 are reserved, 15 is explicitly forbidden by spec
        if sf_index >= 13 {
            return Err(anyhow::Error::msg("ADTS Invalid sampling frequency index"));
        }

        if s.crc_protection_present() {
            s.header_len += 2;
            s.buf.extend_from_slice(&[0, 0]);
            read_exact(stream, &mut s.buf[7..9]).await?;
        }

        if s.frame_length() < s.header_len as u16 {
            return Err(anyhow::Error::msg("ADTS Length of header is different than expected"));
        }

        Ok(s)
    }

    /// 12-bit sync word at the start of the ADTS frame. Must be 0xFFF.
    fn sync_word(&self) -> u16 {
        u16::from(self.buf[0]) << 4 | u16::from(self.buf[1] >> 4)
    }

    /// 2-bit layer field. Per spec, must always be 0.
    fn layer(&self) -> u8 {
        (self.buf[1] >> 1) & 0b11
    }

    /// Whether CRC protection is present (protection_absent bit = 0).
    /// This determines whether the header is 7 or 9 bytes.
    fn crc_protection_present(&self) -> bool {
        self.buf[1] & 0b0000_0001 == 0
    }

    /// 4-bit sampling frequency index. Values 13–14 are reserved, 15 is forbidden.
    fn sampling_frequency_index(&self) -> u8 {
        (self.buf[2] >> 2) & 0b1111
    }

    /// 13-bit frame length: total size of the ADTS frame including header and CRC.
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
        // Read a complete ADTS frame. When number_of_raw_data_blocks > 0, the frame
        // contains multiple AAC packets (RDBs). We forward the complete frame as-is
        // for broadcast — the listeners' decoders handle multi-RDB frames internally.
        // Splitting would require full AAC bitstream parsing which is not feasible here.
        //
        // https://wiki.multimedia.cx/index.php/ADTS recommends one AAC packet per ADTS
        // frame for maximum compatibility, but some encoders (e.g. certain FDK-AAC configs)
        // produce multi-RDB frames that we must accept.
        let mut header = AdtsHeader::read(self.stream).await?;
        let frame_len = header.frame_length() as usize;
        if frame_len < header.header_len {
            return Err(anyhow::Error::msg("ADTS Invalid frame length"));
        }

        // TODO: More efficient way for allocation here
        let mut ret = vec![0; frame_len - header.header_len];
        read_exact(self.stream, &mut ret).await?;

        header.buf.append(&mut ret);
        Ok(header.buf)
    }
}

/// Reads exactly `buf.len()` bytes from the stream, handling partial reads.
/// Returns an error if the stream ends before the buffer is fully filled,
/// or if an I/O error occurs.
async fn read_exact(stream: &mut dyn StreamReader, buf: &mut [u8]) -> Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        match stream.async_read(&mut buf[offset..]).await {
            Ok(0) => return Err(anyhow::Error::msg("ADTS unexpected end of stream")),
            Ok(n) => offset += n,
            Err(_) => return Err(anyhow::Error::msg("ADTS IO error")),
        }
    }
    Ok(())
}
