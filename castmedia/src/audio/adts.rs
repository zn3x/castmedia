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

    /// Number of AAC Raw Data Blocks (RDBs) minus 1 in this frame.
    ///
    /// - 0 → 1 RDB (the common case, recommended by spec for max compatibility)
    /// - 1 → 2 RDBs
    /// - 2 → 3 RDBs
    /// - 3 → 4 RDBs
    ///
    /// When multiple RDBs are present, the payload structure is:
    /// ```text
    /// [raw_data_block #1] [rdb_end #1] [raw_data_block #2] [rdb_end #2] ... [raw_data_block #N]
    /// ```
    ///
    /// For broadcast, we forward the complete ADTS frame as-is. The listeners'
    /// decoders parse the RDB boundaries from the AAC bitstream internally.
    /// We do not need (and cannot reliably) split RDBs without a full AAC parser.
    #[cfg(test)]
    pub fn number_of_raw_data_blocks(&self) -> u8 {
        self.buf[6] & 0b11
    }

    /// Total number of AAC frames (Raw Data Blocks) in this ADTS frame.
    /// Equal to `number_of_raw_data_blocks() + 1`.
    #[cfg(test)]
    pub fn aac_frame_count(&self) -> u8 {
        self.number_of_raw_data_blocks() + 1
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

#[cfg(test)]
mod tests {
    use crate::server::PassFD;

    use super::*;
    use std::future::Future;
    use std::io::{Cursor, Read};
    use std::pin::Pin;

    struct MockStreamReader {
        data: Cursor<Vec<u8>>,
    }

    impl MockStreamReader {
        fn new(data: Vec<u8>) -> Self {
            Self { data: Cursor::new(data) }
        }
    }

    impl Read for MockStreamReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.data.read(buf)
        }
    }

    impl PassFD for MockStreamReader {
        fn fd(&self) -> i32 { -1 }
    }

    impl StreamReader for MockStreamReader {
        fn async_read<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = std::io::Result<usize>> + '_ + Send>> {
            Box::pin(async move { self.data.read(buf) })
        }
    }

    /// Always returns I/O errors.
    struct ErrorMockStreamReader;

    impl Read for ErrorMockStreamReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "mock IO error"))
        }
    }
    
    impl PassFD for ErrorMockStreamReader {
        fn fd(&self) -> i32 { -1 }
    }

    impl StreamReader for ErrorMockStreamReader {
        fn async_read<'a>(
            &'a mut self,
            _buf: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = std::io::Result<usize>> + '_ + Send>> {
            Box::pin(async {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "mock IO error"))
            })
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

    impl Read for ShortReadMockStreamReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let pos = self.data.position() as usize;
            let data = self.data.get_ref();
            if pos >= data.len() {
                return Ok(0);
            }
            buf[0] = data[pos];
            self.data.set_position(pos as u64 + 1);
            Ok(1)
        }
    }

    impl PassFD for ShortReadMockStreamReader {
        fn fd(&self) -> i32 { -1 }
    }

    impl StreamReader for ShortReadMockStreamReader {
        fn async_read<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = std::io::Result<usize>> + '_ + Send>> {
            Box::pin(async move { <Self as Read>::read(self, buf) })
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
        let mut stream = MockStreamReader::new(build_valid_header(20));
        let h = AdtsHeader::read(&mut stream).await.unwrap();
        assert_eq!(h.header_len, 7);
    }

    #[tokio::test]
    async fn test_header_with_crc_is_9_bytes() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let h = AdtsHeader::read(&mut stream).await.unwrap();
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
        let header = build_valid_header(8191);
        let mut stream = MockStreamReader::new(header);
        let h = AdtsHeader::read(&mut stream).await.unwrap();
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
            let mut stream = MockStreamReader::new(header);
            let h = AdtsHeader::read(&mut stream).await.unwrap();
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
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Bad sync word"));
    }

    #[tokio::test]
    async fn test_bad_sync_word_byte1_upper_nibble() {
        let mut header = build_valid_header(50);
        header[1] &= 0x0F;
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Bad sync word"));
    }

    #[tokio::test]
    async fn test_zero_bytes_sync_word() {
        let header = vec![0x00; 7];
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Bad sync word"));
    }

    // ===========================================================================
    // LAYER
    // ===========================================================================

    #[tokio::test]
    async fn test_invalid_layer_1() {
        let header = build_adts_header(0, 1, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid layer"));
    }

    #[tokio::test]
    async fn test_invalid_layer_2() {
        let header = build_adts_header(0, 2, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid layer"));
    }

    #[tokio::test]
    async fn test_invalid_layer_3() {
        let header = build_adts_header(0, 3, 1, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid layer"));
    }

    // ===========================================================================
    // SAMPLING FREQUENCY INDEX
    // ===========================================================================

    #[tokio::test]
    async fn test_forbidden_sampling_frequency_15() {
        let header = build_adts_header(0, 0, 1, 1, 15, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid sampling frequency"));
    }

    #[tokio::test]
    async fn test_reserved_sampling_frequency_13() {
        let header = build_adts_header(0, 0, 1, 1, 13, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid sampling frequency"));
    }

    #[tokio::test]
    async fn test_reserved_sampling_frequency_14() {
        let header = build_adts_header(0, 0, 1, 1, 14, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
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
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    #[tokio::test]
    async fn test_frame_length_less_than_7byte_header() {
        let header = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 5, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    #[tokio::test]
    async fn test_frame_length_less_than_9byte_header() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 8, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    #[tokio::test]
    async fn test_frame_length_6_with_crc() {
        let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 6, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Length of header is different than expected"));
    }

    // ===========================================================================
    // I/O ERRORS
    // ===========================================================================

    #[tokio::test]
    async fn test_io_error_on_header_read() {
        let mut stream = ErrorMockStreamReader;
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }

    #[tokio::test]
    async fn test_io_error_on_crc_read() {
        let mut header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, 50, 0x7FF, 0);
        header.truncate(7);
        let mut stream = MockStreamReader::new(header);
        let result = AdtsHeader::read(&mut stream).await;
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
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_partial_header_only_3_bytes() {
        let mut stream = MockStreamReader::new(vec![0xFF, 0xF1, 0x50]);
        let result = AdtsHeader::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_partial_header_6_of_7_bytes() {
        let data = vec![0xFF, 0xF1, 0x50, 0x80, 0x02, 0x00];
        let mut stream = MockStreamReader::new(data);
        let result = AdtsHeader::read(&mut stream).await;
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
            let mut stream = MockStreamReader::new(header);
            let h = AdtsHeader::read(&mut stream).await.unwrap();
            assert_eq!(h.frame_length(), fl, "frame_length roundtrip failed for {}", fl);
        }
    }

    #[tokio::test]
    async fn test_frame_length_with_crc_roundtrip() {
        for fl in [9u16, 13, 42, 100, 8191] {
            let header = build_adts_header(0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 0, fl, 0x7FF, 0);
            let mut stream = MockStreamReader::new(header);
            let h = AdtsHeader::read(&mut stream).await.unwrap();
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
            let mut stream = MockStreamReader::new(header);
            let h = AdtsHeader::read(&mut stream).await.unwrap();
            assert_eq!(h.frame_length(), 20, "profile {} should parse", profile);
        }
    }

    #[tokio::test]
    async fn test_various_channel_configs() {
        for ch in 0..=7u8 {
            let header = build_adts_header(0, 0, 1, 1, 4, 0, ch, 0, 0, 0, 0, 20, 0x7FF, 0);
            let mut stream = MockStreamReader::new(header);
            let h = AdtsHeader::read(&mut stream).await.unwrap();
            assert_eq!(h.frame_length(), 20, "channel_config {} should parse", ch);
        }
    }

    #[tokio::test]
    async fn test_informational_bits_set() {
        let header = build_adts_header(0, 0, 1, 1, 4, 1, 2, 1, 1, 1, 1, 20, 0x7FF, 0);
        let mut stream = MockStreamReader::new(header);
        let h = AdtsHeader::read(&mut stream).await.unwrap();
        assert_eq!(h.frame_length(), 20);
    }

    #[tokio::test]
    async fn test_buffer_fullness_cbr_and_vbr() {
        let vbr = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x7FF, 0);
        let cbr = build_adts_header(0, 0, 1, 1, 4, 0, 2, 0, 0, 0, 0, 20, 0x100, 0);
        for h_bytes in [&vbr[..], &cbr[..]] {
            let mut stream = MockStreamReader::new(h_bytes.to_vec());
            let h = AdtsHeader::read(&mut stream).await.unwrap();
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
        let mut stream = MockStreamReader::new(header);
        let h = AdtsHeader::read(&mut stream).await.unwrap();
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
        let h = AdtsHeader::read(&mut MockStreamReader::new(result[..7].to_vec())).await.unwrap();
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
        let h = AdtsHeader::read(&mut MockStreamReader::new(result[..7].to_vec())).await.unwrap();
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
        let h = AdtsHeader::read(&mut MockStreamReader::new(result[..7].to_vec())).await.unwrap();
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
        let h1 = AdtsHeader::read(&mut MockStreamReader::new(result1[..7].to_vec())).await.unwrap();
        assert_eq!(h1.aac_frame_count(), 2);

        let result2 = reader.read().await.unwrap();
        assert_eq!(result2, single_frame);
        let h2 = AdtsHeader::read(&mut MockStreamReader::new(result2[..7].to_vec())).await.unwrap();
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

    // ===========================================================================
    // read_exact UNIT TESTS
    // ===========================================================================

    #[tokio::test]
    async fn test_read_exact_success() {
        let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let mut stream = MockStreamReader::new(data.clone());
        let mut buf = vec![0u8; 5];
        read_exact(&mut stream, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_read_exact_zero_length() {
        let mut stream = MockStreamReader::new(vec![0x01]);
        let mut buf = vec![0u8; 0];
        read_exact(&mut stream, &mut buf).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_exact_short_stream() {
        let mut stream = MockStreamReader::new(vec![0x01, 0x02]);
        let mut buf = vec![0u8; 5];
        let result = read_exact(&mut stream, &mut buf).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_read_exact_io_error() {
        let mut stream = ErrorMockStreamReader;
        let mut buf = vec![0u8; 5];
        let result = read_exact(&mut stream, &mut buf).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }

    #[tokio::test]
    async fn test_read_exact_with_short_reads() {
        let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let mut stream = ShortReadMockStreamReader::new(data.clone());
        let mut buf = vec![0u8; 5];
        read_exact(&mut stream, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }
}
