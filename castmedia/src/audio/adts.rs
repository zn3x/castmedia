use anyhow::Result;

use tokio::io::{AsyncRead, AsyncReadExt};
use crate::audio::AudioReader;

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

    /// Validate the first 7 bytes of an ADTS header synchronously.
    ///
    /// Checks the sync word, layer, and sampling frequency index.
    /// Returns a partially-constructed `AdtsHeader` with `header_len` set
    /// to 7 (no CRC) or 9 (CRC present) based on the protection_absent bit.
    fn validate(buf: &[u8; 7]) -> Result<Self> {
        let sync = u16::from(buf[0]) << 4 | u16::from(buf[1] >> 4);
        if sync != 0xfff {
            return Err(anyhow::Error::msg("ADTS Bad sync word"));
        }

        let layer = (buf[1] >> 1) & 0b11;
        if layer != 0 {
            return Err(anyhow::Error::msg("ADTS Invalid layer"));
        }

        let sf_index = (buf[2] >> 2) & 0b1111;
        // Indices 13 and 14 are reserved, 15 is explicitly forbidden by spec
        if sf_index >= 13 {
            return Err(anyhow::Error::msg("ADTS Invalid sampling frequency index"));
        }

        let header_len = if buf[1] & 0b0000_0001 == 0 {
            9 // CRC protection present
        } else {
            7 // No CRC
        };

        Ok(Self {
            header_len,
            buf: buf.to_vec(),
        })
    }

    /// Whether CRC protection is present (protection_absent bit = 0).
    /// This determines whether the header is 7 or 9 bytes.
    fn crc_protection_present(&self) -> bool {
        self.buf[1] & 0b0000_0001 == 0
    }

    /// 13-bit frame length: total size of the ADTS frame including header and CRC.
    pub fn frame_length(&self) -> u16 {
        let buf = &self.buf;
        u16::from(buf[3] & 0b11) << 11
            | u16::from(buf[4]) << 3
            | u16::from(buf[5]) >> 5
    }

    /// 4-bit sampling frequency index. Values 13–14 are reserved, 15 is forbidden.
    #[cfg(test)]
    fn sampling_frequency_index(&self) -> u8 {
        (self.buf[2] >> 2) & 0b1111
    }

    /// Number of AAC Raw Data Blocks (RDBs) minus 1 in this frame.
    #[cfg(test)]
    fn number_of_raw_data_blocks(&self) -> u8 {
        self.buf[6] & 0b11
    }

    /// Total number of AAC frames (Raw Data Blocks) in this ADTS frame.
    #[cfg(test)]
    fn aac_frame_count(&self) -> u8 {
        self.number_of_raw_data_blocks() + 1
    }
}

/// State machine for reading ADTS frames.
enum AdtsReadState {
    /// Reading the fixed 7-byte ADTS header.
    ///
    /// `buf` accumulates header bytes and `filled` tracks how many are valid (0..7).
    HeaderFixed {
        buf: [u8; 7],
        filled: usize,
    },
    /// Reading the 2-byte CRC after the 7-byte header has been validated.
    ///
    /// `buf[0..7]` contains the validated header bytes. `filled` starts at 7
    /// and goes to 9 as CRC bytes are read into `buf[7..9]`.
    HeaderCrc {
        buf: [u8; 9],
        filled: usize,
    },
    /// Header complete (7 or 9 bytes), reading the remaining payload.
    ///
    /// `buf` is pre-allocated to `frame_length` bytes with the header already
    /// filled in. `filled` tracks total bytes written (starts at `header_len`,
    /// goes up to `frame_length`).
    Payload {
        buf: Vec<u8>,
        frame_length: usize,
        filled: usize,
    },
}

pub struct AdtsReader<'a, R: AsyncRead + Unpin> {
    stream: &'a mut R,
    state: AdtsReadState,
}

impl<'a, R: AsyncRead + Unpin> AdtsReader<'a, R> {
    pub fn new(stream: &'a mut R) -> Self {
        Self {
            stream,
            state: AdtsReadState::HeaderFixed {
                buf: [0u8; 7],
                filled: 0,
            },
        }
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Unpin + Send> AudioReader for AdtsReader<'_, R> {
    /// Read one complete ADTS frame from the stream.
    ///
    /// This method is cancel-safe: if the returned future is dropped at an
    /// `.await` point (e.g., by `tokio::select!`), all progress is preserved
    /// in `self.state`. The next call to `read()` will resume from exactly
    /// where it left off — no bytes from the stream are lost.
    async fn read(&mut self) -> Result<Vec<u8>> {
        loop {
            match &mut self.state {
                AdtsReadState::HeaderFixed { buf, filled } => {
                    if *filled < AdtsHeader::SIZE {
                        // Read bytes into the header buffer until we have 7.
                        // `stream.read()` is cancel-safe: if dropped, no bytes
                        // are consumed. We update `filled` synchronously after
                        // the read returns, so progress is always persisted.
                        match self.stream.read(&mut buf[*filled..AdtsHeader::SIZE]).await {
                            Ok(0) => return Err(anyhow::Error::msg("ADTS unexpected end of stream")),
                            Ok(n) => *filled += n,
                            Err(_) => return Err(anyhow::Error::msg("ADTS IO error")),
                        }
                        continue;
                    }

                    // Have 7 bytes — validate the header fields
                    let header = AdtsHeader::validate(buf)?;

                    if header.crc_protection_present() {
                        // Need 2 more CRC bytes — transition to HeaderCrc.
                        // Copy the 7 header bytes into the 9-byte buffer.
                        let mut crc_buf = [0u8; 9];
                        crc_buf[..7].copy_from_slice(buf);
                        // Transition synchronously — state is preserved if
                        // cancelled at the next .await.
                        self.state = AdtsReadState::HeaderCrc {
                            buf: crc_buf,
                            filled: 7,
                        };
                        continue;
                    }

                    // No CRC — validate frame length and transition to Payload
                    let frame_length = header.frame_length() as usize;
                    if frame_length < header.header_len {
                        return Err(anyhow::Error::msg(
                            "ADTS Length of header is different than expected",
                        ));
                    }

                    let mut frame_buf = Vec::with_capacity(frame_length);
                    frame_buf.extend_from_slice(&header.buf);
                    frame_buf.resize(frame_length, 0);

                    self.state = AdtsReadState::Payload {
                        buf: frame_buf,
                        frame_length,
                        filled: header.header_len,
                    };
                    continue;
                }
                AdtsReadState::HeaderCrc { buf, filled } => {
                    if *filled < 9 {
                        // Read the remaining CRC bytes (buf[7..9]).
                        // Same cancel-safety guarantees: stream.read() is
                        // cancel-safe, and we update `filled` synchronously.
                        match self.stream.read(&mut buf[*filled..9]).await {
                            Ok(0) => return Err(anyhow::Error::msg("ADTS unexpected end of stream")),
                            Ok(n) => *filled += n,
                            Err(_) => return Err(anyhow::Error::msg("ADTS IO error")),
                        }
                        continue;
                    }

                    // CRC bytes read — now we have a complete 9-byte header.
                    // Validate frame length and transition to Payload.
                    let header = AdtsHeader {
                        header_len: 9,
                        buf: buf[..9].to_vec(),
                    };

                    let frame_length = header.frame_length() as usize;
                    if frame_length < header.header_len {
                        return Err(anyhow::Error::msg(
                            "ADTS Length of header is different than expected",
                        ));
                    }

                    let mut frame_buf = Vec::with_capacity(frame_length);
                    frame_buf.extend_from_slice(&header.buf);
                    frame_buf.resize(frame_length, 0);

                    self.state = AdtsReadState::Payload {
                        buf: frame_buf,
                        frame_length,
                        filled: 9,
                    };
                    continue;
                }
                AdtsReadState::Payload { buf, frame_length, filled } => {
                    if *filled < *frame_length {
                        // Read payload bytes. Same cancel-safety guarantees:
                        // stream.read() is cancel-safe, and we update `filled`
                        // synchronously after each read.
                        match self.stream.read(&mut buf[*filled..*frame_length]).await {
                            Ok(0) => return Err(anyhow::Error::msg("ADTS unexpected end of stream")),
                            Ok(n) => *filled += n,
                            Err(_) => return Err(anyhow::Error::msg("ADTS IO error")),
                        }
                        continue;
                    }

                    // Frame is complete — return it and reset state for next frame
                    let result = std::mem::take(buf);
                    self.state = AdtsReadState::HeaderFixed {
                        buf: [0u8; 7],
                        filled: 0,
                    };
                    return Ok(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    struct MockStreamReader {
        data: Cursor<Vec<u8>>,
    }

    impl MockStreamReader {
        fn new(data: Vec<u8>) -> Self {
            Self { data: Cursor::new(data) }
        }
    }

    impl AsyncRead for MockStreamReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let n = std::io::Read::read(&mut this.data, buf.initialize_unfilled())?;
            buf.advance(n);
            Poll::Ready(Ok(()))
        }
    }

    /// Always returns I/O errors.
    struct ErrorMockStreamReader;

    impl AsyncRead for ErrorMockStreamReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, "mock IO error")))
        }
    }

    /// Returns data one byte at a time to simulate short reads.
    struct ShortReadMockStreamReader {
        data: Cursor<Vec<u8>>,
    }

    impl ShortReadMockStreamReader {
        fn new(data: Vec<u8>) -> Self {
            Self { data: Cursor::new(data) }
        }
    }

    impl AsyncRead for ShortReadMockStreamReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            let pos = this.data.position() as usize;
            let data = this.data.get_ref();
            if pos >= data.len() {
                return Poll::Ready(Ok(()));
            }
            let filled = buf.initialize_unfilled();
            filled[0] = data[pos];
            this.data.set_position(pos as u64 + 1);
            buf.advance(1);
            Poll::Ready(Ok(()))
        }
    }

    // ---------------------------------------------------------------------------
    // ADTS header builder
    // ---------------------------------------------------------------------------

    /// Builds raw ADTS header bytes with specified field values.
    ///
    /// Byte layout:
    ///   0: [sync 11:4]               = 0xFF
    ///   1: [sync 3:0][ID][Layer][PA]
    ///   2: [profile][sf_idx][priv][ch MSB]
    ///   3: [ch LSB][orig][home][cpid][cpid_start][flen 12:11]
    ///   4: [flen 10:3]
    ///   5: [flen 2:0][bufull 10:6]
    ///   6: [bufull 5:0][nrdb 1:0]
    ///   7-8: CRC (only if PA=0)
    fn build_adts_header(
        mpeg_version: u8,
        layer: u8,
        protection_absent: u8,
        profile: u8,
        sampling_freq_index: u8,
        private_bit: u8,
        channel_config: u8,
        originality: u8,
        home: u8,
        copyright_id_bit: u8,
        copyright_id_start: u8,
        frame_length: u16,
        buffer_fullness: u16,
        num_raw_data_blocks: u8,
    ) -> Vec<u8> {
        let mut buf = vec![
            0xFF,
            0xF0 | ((mpeg_version & 0b1) << 3) | ((layer & 0b11) << 1) | (protection_absent & 0b1),
            ((profile & 0b11) << 6)
                | ((sampling_freq_index & 0b1111) << 2)
                | ((private_bit & 0b1) << 1)
                | ((channel_config >> 2) & 0b1),
            ((channel_config & 0b11) << 6)
                | ((originality & 0b1) << 5)
                | ((home & 0b1) << 4)
                | ((copyright_id_bit & 0b1) << 3)
                | ((copyright_id_start & 0b1) << 2)
                | ((frame_length >> 11) & 0b11) as u8,
            ((frame_length >> 3) & 0xFF) as u8,
            (((frame_length & 0b111) << 5) | ((buffer_fullness >> 6) & 0b11111)) as u8,
            (((buffer_fullness & 0b111111) << 2) | (num_raw_data_blocks as u16 & 0b11)) as u8,
        ];

        if protection_absent == 0 {
            buf.push(0xAB);
            buf.push(0xCD);
        }

        buf
    }

    /// Convenience: build a typical valid header (MPEG-4, LC, 44100Hz, stereo, no CRC).
    fn build_valid_header(frame_length: u16) -> Vec<u8> {
        build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, frame_length, 0x7FF, 0)
    }

    /// Build a complete ADTS frame (header + dummy payload).
    fn build_adts_frame(header_bytes: &[u8], frame_length: u16) -> Vec<u8> {
        let payload_len = frame_length as usize - header_bytes.len();
        let mut frame = header_bytes.to_vec();
        frame.extend(std::iter::repeat(0xAA).take(payload_len));
        frame
    }

    // ===========================================================================
    // VALID FRAMES
    // ===========================================================================

    #[tokio::test]
    async fn test_valid_frame_without_crc() {
        let header = build_valid_header(100);
        let frame = build_adts_frame(&header, 100);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 100);
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_frame_with_crc() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let frame = build_adts_frame(&header, 50);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 50);
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_header_without_crc_is_7_bytes() {
        let h = AdtsHeader::validate(&build_valid_header(20).try_into().unwrap()).unwrap();
        assert_eq!(h.header_len, 7);
    }

    #[tokio::test]
    async fn test_header_with_crc_is_9_bytes() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let h = AdtsHeader::validate(&header[..7].try_into().unwrap()).unwrap();
        assert_eq!(h.header_len, 9);
    }

    #[tokio::test]
    async fn test_frame_length_equals_header_size_without_crc() {
        let header = build_valid_header(7);
        let frame = build_adts_frame(&header, 7);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 7);
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_frame_length_equals_header_size_with_crc() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 9, 0x7FF, 0);
        let frame = build_adts_frame(&header, 9);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 9);
    }

    #[tokio::test]
    async fn test_frame_length_max_13bit() {
        let h = AdtsHeader::validate(&build_valid_header(8191).try_into().unwrap()).unwrap();
        assert_eq!(h.frame_length(), 8191);
    }

    #[tokio::test]
    async fn test_mpeg2_frame() {
        let header = build_adts_header(1, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
        let frame = build_adts_frame(&header, 20);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_mpeg4_frame() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
        let frame = build_adts_frame(&header, 20);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_all_valid_sampling_frequencies() {
        for idx in 0..=12u8 {
            let header = build_adts_header(0, 0, 1, 1, idx, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
            let h = AdtsHeader::validate(&header.try_into().unwrap()).unwrap();
            assert_eq!(h.sampling_frequency_index(), idx);
        }
    }

    #[tokio::test]
    async fn test_multiple_consecutive_frames() {
        let frame1 = build_adts_frame(&build_valid_header(20), 20);
        let frame2 = build_adts_frame(&build_valid_header(30), 30);

        let mut combined = frame1.clone();
        combined.extend_from_slice(&frame2);

        let mut stream = MockStreamReader::new(combined);
        let mut reader = AdtsReader::new(&mut stream);

        assert_eq!(reader.read().await.unwrap(), frame1);
        assert_eq!(reader.read().await.unwrap(), frame2);
    }

    #[tokio::test]
    async fn test_large_frame() {
        let frame_length: u16 = 4096;
        let header = build_valid_header(frame_length);
        let frame = build_adts_frame(&header, frame_length);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), frame_length as usize);
        assert_eq!(result, frame);
    }

    // ===========================================================================
    // SYNC WORD
    // ===========================================================================

    #[tokio::test]
    async fn test_bad_sync_word_byte0() {
        let mut header = build_valid_header(50);
        header[0] = 0xFE;
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Bad sync word"));
    }

    #[tokio::test]
    async fn test_bad_sync_word_byte1_upper_nibble() {
        let mut header = build_valid_header(50);
        header[1] &= 0x0F;
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Bad sync word"));
    }

    #[tokio::test]
    async fn test_zero_bytes_sync_word() {
        let result = AdtsHeader::validate(&[0x00; 7]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Bad sync word"));
    }

    // ===========================================================================
    // LAYER
    // ===========================================================================

    #[tokio::test]
    async fn test_invalid_layer_1() {
        let header = build_adts_header(0, 1, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid layer"));
    }

    #[tokio::test]
    async fn test_invalid_layer_2() {
        let header = build_adts_header(0, 2, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid layer"));
    }

    #[tokio::test]
    async fn test_invalid_layer_3() {
        let header = build_adts_header(0, 3, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid layer"));
    }

    // ===========================================================================
    // SAMPLING FREQUENCY INDEX
    // ===========================================================================

    #[tokio::test]
    async fn test_forbidden_sampling_frequency_15() {
        let header = build_adts_header(0, 0, 1, 1, 15, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid sampling frequency"));
    }

    #[tokio::test]
    async fn test_reserved_sampling_frequency_13() {
        let header = build_adts_header(0, 0, 1, 1, 13, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid sampling frequency"));
    }

    #[tokio::test]
    async fn test_reserved_sampling_frequency_14() {
        let header = build_adts_header(0, 0, 1, 1, 14, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let result = AdtsHeader::validate(&header.try_into().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid sampling frequency"));
    }

    // ===========================================================================
    // FRAME LENGTH
    // ===========================================================================

    #[tokio::test]
    async fn test_frame_length_zero() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 0, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    #[tokio::test]
    async fn test_frame_length_less_than_7byte_header() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 5, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    #[tokio::test]
    async fn test_frame_length_less_than_9byte_header() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 8, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    #[tokio::test]
    async fn test_frame_length_6_with_crc() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 6, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    // ===========================================================================
    // I/O ERRORS
    // ===========================================================================

    #[tokio::test]
    async fn test_io_error_on_header_read() {
        let mut stream = ErrorMockStreamReader;
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }

    #[tokio::test]
    async fn test_io_error_on_crc_read() {
        let mut header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        header.truncate(7);
        let mut stream = MockStreamReader::new(header);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_io_error_on_payload_read() {
        let header = build_valid_header(100);
        let mut stream = MockStreamReader::new(header);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_reader_io_error() {
        let mut stream = ErrorMockStreamReader;
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }

    // ===========================================================================
    // SHORT READS / PARTIAL DATA
    // ===========================================================================

    #[tokio::test]
    async fn test_empty_stream() {
        let mut stream = MockStreamReader::new(vec![]);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_partial_header_only_3_bytes() {
        let mut stream = MockStreamReader::new(vec![0xFF, 0xF1, 0x50]);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_partial_header_6_of_7_bytes() {
        let data = vec![0xFF, 0xF1, 0x50, 0x80, 0x02, 0x00];
        let mut stream = MockStreamReader::new(data);
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_short_reads_byte_by_byte() {
        let header = build_valid_header(20);
        let frame = build_adts_frame(&header, 20);
        let mut stream = ShortReadMockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_short_reads_with_crc() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 25, 0x7FF, 0);
        let frame = build_adts_frame(&header, 25);
        let mut stream = ShortReadMockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    // ===========================================================================
    // BIT-LEVEL ROUND-TRIP (frame_length is the critical field)
    // ===========================================================================

    #[tokio::test]
    async fn test_frame_length_roundtrip_odd_values() {
        for fl in [7u16, 9, 13, 42, 100, 256, 1024, 4096, 8191] {
            let header = build_valid_header(fl);
            let h = AdtsHeader::validate(&header.try_into().unwrap()).unwrap();
            assert_eq!(h.frame_length(), fl, "frame_length roundtrip failed for {}", fl);
        }
    }

    #[tokio::test]
    async fn test_frame_length_with_crc_roundtrip() {
        for fl in [9u16, 13, 42, 100, 8191] {
            let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, fl, 0x7FF, 0);
            let h = AdtsHeader::validate(&header[..7].try_into().unwrap()).unwrap();
            assert_eq!(h.frame_length(), fl, "frame_length roundtrip (CRC) failed for {}", fl);
        }
    }

    // ===========================================================================
    // VARIOUS VALID CONFIGS (broadcast doesn't interpret these, but must accept)
    // ===========================================================================

    #[tokio::test]
    async fn test_various_profiles() {
        for profile in 0..=3u8 {
            let header = build_adts_header(0, 0, 1, profile, 4, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
            let h = AdtsHeader::validate(&header.try_into().unwrap()).unwrap();
            assert_eq!(h.frame_length(), 20, "profile {} should parse", profile);
        }
    }

    #[tokio::test]
    async fn test_various_channel_configs() {
        for ch in 0..=7u8 {
            let header = build_adts_header(0, 0, 1, 1, 4, 0, ch, 0, 0, 0, 0, 20, 0x7FF, 0);
            let h = AdtsHeader::validate(&header.try_into().unwrap()).unwrap();
            assert_eq!(h.frame_length(), 20, "channel_config {} should parse", ch);
        }
    }

    #[tokio::test]
    async fn test_informational_bits_set() {
        let header = build_adts_header(0, 0, 1, 1, 4, 1, 2, 1, 1, 1, 1, 20, 0x7FF, 0);
        let h = AdtsHeader::validate(&header.try_into().unwrap()).unwrap();
        assert_eq!(h.frame_length(), 20);
    }

    #[tokio::test]
    async fn test_buffer_fullness_cbr_and_vbr() {
        let vbr = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
        let cbr = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x100, 0);
        for h_bytes in [&vbr[..], &cbr[..]] {
            let h = AdtsHeader::validate(&h_bytes.try_into().unwrap()).unwrap();
            assert_eq!(h.frame_length(), 20);
        }
    }

    // ===========================================================================
    // MULTIPLE AAC PACKETS PER ADTS FRAME (number_of_raw_data_blocks)
    // ===========================================================================

    #[tokio::test]
    async fn test_single_rdb_nrdb_zero() {
        // nrdb=0 → 1 AAC frame (the common case)
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
        let h = AdtsHeader::validate(&header.try_into().unwrap()).unwrap();
        assert_eq!(h.number_of_raw_data_blocks(), 0);
        assert_eq!(h.aac_frame_count(), 1);
    }

    #[tokio::test]
    async fn test_two_rdbs_nrdb_one() {
        // nrdb=1 → 2 AAC frames in one ADTS frame
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 30, 0x7FF, 1);
        let frame = build_adts_frame(&header, 30);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
        let h = AdtsHeader::validate(&result[..7].try_into().unwrap()).unwrap();
        assert_eq!(h.number_of_raw_data_blocks(), 1);
        assert_eq!(h.aac_frame_count(), 2);
    }

    #[tokio::test]
    async fn test_three_rdbs_nrdb_two() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 40, 0x7FF, 2);
        let frame = build_adts_frame(&header, 40);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
        let h = AdtsHeader::validate(&result[..7].try_into().unwrap()).unwrap();
        assert_eq!(h.number_of_raw_data_blocks(), 2);
        assert_eq!(h.aac_frame_count(), 3);
    }

    #[tokio::test]
    async fn test_four_rdbs_nrdb_three() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 3);
        let frame = build_adts_frame(&header, 50);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
        let h = AdtsHeader::validate(&result[..7].try_into().unwrap()).unwrap();
        assert_eq!(h.number_of_raw_data_blocks(), 3);
        assert_eq!(h.aac_frame_count(), 4);
    }

    #[tokio::test]
    async fn test_multi_rdb_with_crc() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 40, 0x7FF, 2);
        let frame = build_adts_frame(&header, 40);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_multi_rdb_consecutive_with_single_rdb() {
        let multi_header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 30, 0x7FF, 1);
        let multi_frame = build_adts_frame(&multi_header, 30);
        let single_header = build_valid_header(20);
        let single_frame = build_adts_frame(&single_header, 20);

        let mut combined = multi_frame.clone();
        combined.extend_from_slice(&single_frame);

        let mut stream = MockStreamReader::new(combined);
        let mut reader = AdtsReader::new(&mut stream);

        let result1 = reader.read().await.unwrap();
        assert_eq!(result1, multi_frame);
        let h1 = AdtsHeader::validate(&result1[..7].try_into().unwrap()).unwrap();
        assert_eq!(h1.aac_frame_count(), 2);

        let result2 = reader.read().await.unwrap();
        assert_eq!(result2, single_frame);
        let h2 = AdtsHeader::validate(&result2[..7].try_into().unwrap()).unwrap();
        assert_eq!(h2.aac_frame_count(), 1);
    }

    #[tokio::test]
    async fn test_multi_rdb_short_reads() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 30, 0x7FF, 1);
        let frame = build_adts_frame(&header, 30);
        let mut stream = ShortReadMockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_multi_rdb_large_frame() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 4096, 0x7FF, 3);
        let frame = build_adts_frame(&header, 4096);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = AdtsReader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 4096);
        assert_eq!(result, frame);
    }

}
