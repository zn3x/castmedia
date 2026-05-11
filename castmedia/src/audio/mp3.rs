use anyhow::Result;

use crate::{audio::AudioReader, stream::StreamReader};

/// MPEG Audio frame header for MP3 streaming.
///
/// MP3 (MPEG Audio) is a self-framing format — each frame contains its own
/// header with enough information to determine the frame boundaries. Unlike
/// ADTS/AAC, there is no explicit frame-length field; the size must be
/// calculated from the header parameters.
///
/// Frame header layout (4 bytes, 32 bits):
/// ```text
/// AAAAAAAA AAABBCCD EEEEFFGH IIJJKLMM
/// ```
///
/// For broadcast, we only need fields required to correctly delimit frames
/// and detect corrupt data. The listeners' MP3 decoders handle everything else.
///
/// | Letters | Bits | Used? | Why                                               |
/// |---------|------|-------|---------------------------------------------------|
/// | A       | 11   | YES   | Sync word — required to find frame start          |
/// | B       | 2    | YES   | MPEG version — needed for frame size, 01=reserved |
/// | C       | 2    | YES   | Layer — needed for frame size, 00=reserved        |
/// | D       | 1    | no    | Protection — CRC is part of the payload           |
/// | E       | 4    | YES   | Bitrate index — needed for frame size, 0xF=bad    |
/// | F       | 2    | YES   | Sample rate index — needed for frame size, 11=bad |
/// | G       | 1    | YES   | Padding — affects frame size by 1 slot            |
/// | H       | 1    | no    | Private — informational only                      |
/// | I       | 2    | no    | Channel mode — decoder's job                      |
/// | J       | 2    | no    | Mode extension — decoder's job                    |
/// | K       | 1    | no    | Copyright — informational only                    |
/// | L       | 1    | no    | Original — informational only                     |
/// | M       | 2    | no    | Emphasis — decoder's job                          |
///
/// References:
/// - ISO/IEC 11172-3 (MPEG-1 Audio)
/// - ISO/IEC 13818-3 (MPEG-2 Audio)
/// - https://wiki.multimedia.cx/index.php/MP3

#[derive(Debug)]
pub struct Mp3Header {
    buf: [u8; 4],
}

impl Mp3Header {
    const SIZE: usize = 4;

    /// Bitrate tables (kbps). Indexed by [version][layer][bitrate_index].
    ///
    /// version: 0 = MPEG 2.5, 1 = reserved, 2 = MPEG 2, 3 = MPEG 1
    /// layer: 0 = reserved, 1 = Layer III, 2 = Layer II, 3 = Layer I
    ///
    /// Index 0 = "free format" (unsupported — frame size indeterminable without
    /// scanning for the next sync word), index 15 = "bad" (forbidden by spec).
    /// Both are stored as 0 and rejected during validation.
    const BITRATE_TABLE: [[[u16; 16]; 4]; 4] = [
        // MPEG 2.5
        [
            [0; 16], // reserved layer
            [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0], // Layer III
            [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0], // Layer II
            [0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256, 0], // Layer I
        ],
        // Reserved version
        [[0; 16]; 4],
        // MPEG 2
        [
            [0; 16], // reserved layer
            [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0], // Layer III
            [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0], // Layer II
            [0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256, 0], // Layer I
        ],
        // MPEG 1
        [
            [0; 16], // reserved layer
            [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0], // Layer III
            [0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 0], // Layer II
            [0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, 0], // Layer I
        ],
    ];

    /// Sampling rate tables (Hz). Indexed by [version][sample_rate_index].
    /// Index 3 = reserved (stored as 0, rejected during validation).
    const SAMPLE_RATE_TABLE: [[u32; 4]; 4] = [
        [11025, 12000, 8000, 0], // MPEG 2.5
        [0; 4],                   // Reserved version
        [22050, 24000, 16000, 0], // MPEG 2
        [44100, 48000, 32000, 0], // MPEG 1
    ];

    /// Slots per frame for frame size calculation. Indexed by [version][layer].
    ///
    /// - Layer I:  each slot is 4 bytes (frame_size = (slots * bitrate / sample_rate + padding) * 4)
    /// - Layer II/III: each slot is 1 byte (frame_size = slots * bitrate / sample_rate + padding)
    const SLOTS_PER_FRAME: [[u32; 4]; 4] = [
        [0, 72, 72, 6],    // MPEG 2.5
        [0; 4],            // Reserved version
        [0, 72, 72, 6],    // MPEG 2
        [0, 144, 144, 12], // MPEG 1
    ];

    /// Read an MP3 frame header from the stream.
    ///
    /// If the stream is not aligned to a frame boundary (e.g., after corruption
    /// or an ID3 tag), this method scans forward byte-by-byte until a valid
    /// header is found. This is essential for broadcast resilience — a single
    /// corrupt frame or an ID3 preamble should not disconnect the source.
    ///
    /// Note: ID3v2 tag detection and skipping is not implemented here. For
    /// streams that start with large ID3 tags, the byte-by-byte scan will
    /// eventually find the first MP3 frame. A future optimization could detect
    /// the "ID3" magic and skip the tag using its encoded size.
    pub async fn read(stream: &mut dyn StreamReader) -> Result<Self> {
        let mut buf = [0u8; Self::SIZE];
        read_exact(stream, &mut buf).await?;

        loop {
            // Check for sync word: 11 set bits (0xFF followed by upper 3 bits set)
            if buf[0] == 0xFF && (buf[1] & 0xE0) == 0xE0 {
                let version = (buf[1] >> 3) & 0b11;
                let layer = (buf[1] >> 1) & 0b11;
                let bitrate_index = (buf[2] >> 4) & 0b1111;
                let sampling_rate_index = (buf[2] >> 2) & 0b11;

                // Validate all critical fields for frame delimiting
                if version != 0b01              // not reserved
                    && layer != 0b00            // not reserved
                    && bitrate_index != 0b1111  // not "bad" (forbidden by spec)
                    && bitrate_index != 0b0000  // not "free format" (indeterminable frame size)
                    && sampling_rate_index != 0b11 // not reserved
                {
                    let header = Self { buf };
                    // Verify the bitrate/sample rate lookup yields valid values
                    // for this version/layer combination
                    if header.bitrate_kbps() != 0
                        && header.sampling_rate_hz() != 0
                        && header.frame_size() >= Self::SIZE
                    {
                        return Ok(header);
                    }
                }
            }

            // Shift buffer left by 1 byte and read a new byte to continue scanning
            buf[0] = buf[1];
            buf[1] = buf[2];
            buf[2] = buf[3];
            read_exact(stream, &mut buf[3..4]).await?;
        }
    }

    /// MPEG Audio version ID (2 bits):
    /// 00 = MPEG 2.5, 01 = reserved, 10 = MPEG 2, 11 = MPEG 1
    fn mpeg_version(&self) -> u8 {
        (self.buf[1] >> 3) & 0b11
    }

    /// Layer description (2 bits):
    /// 00 = reserved, 01 = Layer III, 10 = Layer II, 11 = Layer I
    fn layer(&self) -> u8 {
        (self.buf[1] >> 1) & 0b11
    }

    /// 4-bit bitrate index.
    fn bitrate_index(&self) -> u8 {
        (self.buf[2] >> 4) & 0b1111
    }

    /// 2-bit sampling rate frequency index.
    fn sampling_rate_index(&self) -> u8 {
        (self.buf[2] >> 2) & 0b11
    }

    /// Padding bit (1 bit). When set, one extra slot is added to the frame.
    fn padding(&self) -> bool {
        self.buf[2] & 0b0000_0010 != 0
    }

    /// Bitrate in kbps for this frame, looked up from the bitrate table.
    fn bitrate_kbps(&self) -> u16 {
        Self::BITRATE_TABLE[self.mpeg_version() as usize][self.layer() as usize]
            [self.bitrate_index() as usize]
    }

    /// Sampling rate in Hz for this frame, looked up from the sample rate table.
    fn sampling_rate_hz(&self) -> u32 {
        Self::SAMPLE_RATE_TABLE[self.mpeg_version() as usize][self.sampling_rate_index() as usize]
    }

    /// Calculate the total frame size in bytes (header + payload, including CRC
    /// if present).
    ///
    /// Layer I:      `(slots_per_frame * bitrate / sample_rate + padding) * 4`
    /// Layer II/III: `slots_per_frame * bitrate / sample_rate + padding`
    pub fn frame_size(&self) -> usize {
        let bitrate = self.bitrate_kbps() as u32 * 1000;
        let sample_rate = self.sampling_rate_hz();
        let padding_val = if self.padding() { 1 } else { 0 };
        let slots = Self::SLOTS_PER_FRAME[self.mpeg_version() as usize][self.layer() as usize];

        if self.layer() == 3 {
            // Layer I: each slot is 4 bytes
            (((slots * bitrate / sample_rate) + padding_val) * 4) as usize
        } else {
            // Layer II & III: each slot is 1 byte
            ((slots * bitrate / sample_rate) + padding_val) as usize
        }
    }
}

pub struct Mp3Reader<'a> {
    stream: &'a mut dyn StreamReader,
}

impl<'a> Mp3Reader<'a> {
    pub fn new(stream: &'a mut dyn StreamReader) -> Self {
        Self { stream }
    }
}

#[async_trait::async_trait]
impl AudioReader for Mp3Reader<'_> {
    async fn read(&mut self) -> Result<Vec<u8>> {
        let header = Mp3Header::read(self.stream).await?;
        let frame_size = header.frame_size();

        // frame_size includes the 4-byte header
        let mut frame = vec![0u8; frame_size];
        frame[..4].copy_from_slice(&header.buf);
        read_exact(self.stream, &mut frame[4..]).await?;

        Ok(frame)
    }
}

/// Reads exactly `buf.len()` bytes from the stream, handling partial reads.
/// Returns an error if the stream ends before the buffer is fully filled,
/// or if an I/O error occurs.
async fn read_exact(stream: &mut dyn StreamReader, buf: &mut [u8]) -> Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        match stream.async_read(&mut buf[offset..]).await {
            Ok(0) => return Err(anyhow::Error::msg("MP3 unexpected end of stream")),
            Ok(n) => offset += n,
            Err(_) => return Err(anyhow::Error::msg("MP3 IO error")),
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
            Self {
                data: Cursor::new(data),
            }
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
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "mock IO error",
            ))
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
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "mock IO error",
                ))
            })
        }
    }

    /// Returns data one byte at a time to simulate short reads.
    struct ShortReadMockStreamReader {
        data: Cursor<Vec<u8>>,
    }

    impl ShortReadMockStreamReader {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Cursor::new(data),
            }
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
    // MP3 header & frame builders
    // ---------------------------------------------------------------------------

    /// Builds a raw 4-byte MP3 frame header with the specified field values.
    ///
    /// Byte layout:
    ///   0: [sync 11:4] = 0xFF
    ///   1: [sync 3:0][version 1:0][layer 1:0][protection]
    ///   2: [bitrate_index 3:0][sample_rate_index 1:0][padding][private]
    ///   3: [channel_mode 1:0][mode_ext 1:0][copyright][original][emphasis 1:0]
    fn build_mp3_header(
        version: u8,
        layer: u8,
        protection: bool,
        bitrate_index: u8,
        sample_rate_index: u8,
        padding: bool,
    ) -> [u8; 4] {
        let byte1 =
            0xE0 | (version << 3) | (layer << 1) | if protection { 0 } else { 1 };
        let byte2 = (bitrate_index << 4)
            | (sample_rate_index << 2)
            | if padding { 0b10 } else { 0 };
        [0xFF, byte1, byte2, 0x00]
    }

    /// Build a complete MP3 frame (header + dummy payload) with the given
    /// parameters. The frame size is calculated using the same tables as
    /// the production code.
    fn build_mp3_frame(
        version: u8,
        layer: u8,
        protection: bool,
        bitrate_index: u8,
        sample_rate_index: u8,
        padding: bool,
    ) -> Vec<u8> {
        let header = build_mp3_header(
            version,
            layer,
            protection,
            bitrate_index,
            sample_rate_index,
            padding,
        );
        let bitrate_kbps =
            Mp3Header::BITRATE_TABLE[version as usize][layer as usize][bitrate_index as usize];
        let sample_rate =
            Mp3Header::SAMPLE_RATE_TABLE[version as usize][sample_rate_index as usize];
        let slots = Mp3Header::SLOTS_PER_FRAME[version as usize][layer as usize];
        let bitrate_bps = bitrate_kbps as u32 * 1000;
        let padding_val = if padding { 1 } else { 0 };

        let frame_size: usize = if layer == 3 {
            (((slots * bitrate_bps / sample_rate) + padding_val) * 4) as usize
        } else {
            ((slots * bitrate_bps / sample_rate) + padding_val) as usize
        };

        let mut frame = vec![0xAA; frame_size];
        frame[..4].copy_from_slice(&header);
        frame
    }

    /// Convenience: MPEG 1, Layer III, 128kbps, 44100Hz, no CRC, no padding.
    /// Produces the classic 417-byte MP3 frame.
    fn build_valid_header() -> [u8; 4] {
        build_mp3_header(3, 1, false, 9, 0, false)
    }

    fn build_valid_frame() -> Vec<u8> {
        build_mp3_frame(3, 1, false, 9, 0, false)
    }

    // ===========================================================================
    // VALID FRAMES
    // ===========================================================================

    #[tokio::test]
    async fn test_valid_mpeg1_layer3_128kbps_44100hz() {
        // The classic MP3 frame: MPEG1 Layer III 128kbps 44100Hz
        // Expected frame size: 144 * 128000 / 44100 = 417 bytes
        let frame = build_mp3_frame(3, 1, false, 9, 0, false);
        assert_eq!(frame.len(), 417);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 417);
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_mpeg1_layer3_320kbps_44100hz() {
        // Highest common bitrate: 144 * 320000 / 44100 = 1044 bytes
        let frame = build_mp3_frame(3, 1, false, 14, 0, false);
        assert_eq!(frame.len(), 1044);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), 1044);
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_mpeg1_layer3_32kbps_44100hz() {
        // Lowest MPEG1 Layer III bitrate: 144 * 32000 / 44100 = 104 bytes
        let frame = build_mp3_frame(3, 1, false, 1, 0, false);
        assert_eq!(frame.len(), 104);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_mpeg1_layer2() {
        // bitrate_index=8 → 128kbps for Layer II (tables differ per layer)
        let frame = build_mp3_frame(3, 2, false, 8, 0, false);
        // MPEG1 Layer II 128kbps 44100Hz: 144 * 128000 / 44100 = 417
        assert_eq!(frame.len(), 417);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_mpeg1_layer1() {
        let frame = build_mp3_frame(3, 3, false, 4, 0, false);
        // MPEG1 Layer I 128kbps 44100Hz: (12 * 128000 / 44100) * 4 = 34 * 4 = 136
        assert_eq!(frame.len(), 136);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_mpeg2_layer3() {
        // bitrate_index=8 → 64kbps for MPEG2 Layer III
        let frame = build_mp3_frame(2, 1, false, 8, 0, false);
        // MPEG2 Layer III 64kbps 22050Hz: 72 * 64000 / 22050 = 208
        assert_eq!(frame.len(), 208);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_valid_mpeg2_5_layer3() {
        // bitrate_index=8 → 64kbps for MPEG2.5 Layer III
        let frame = build_mp3_frame(0, 1, false, 8, 0, false);
        // MPEG2.5 Layer III 64kbps 11025Hz: 72 * 64000 / 11025 = 417
        assert_eq!(frame.len(), 417);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_with_crc_protection() {
        let frame = build_mp3_frame(3, 1, true, 9, 0, false);
        // Frame size is the same regardless of CRC — CRC is part of the payload
        assert_eq!(frame.len(), 417);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_with_padding() {
        let frame_no_pad = build_mp3_frame(3, 1, false, 9, 0, false);
        let frame_with_pad = build_mp3_frame(3, 1, false, 9, 0, true);
        // Layer III: padding adds 1 byte
        assert_eq!(frame_with_pad.len(), frame_no_pad.len() + 1);

        let mut stream = MockStreamReader::new(frame_with_pad.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame_with_pad);
    }

    #[tokio::test]
    async fn test_layer1_padding_adds_4_bytes() {
        // Layer I: padding adds 4 bytes (each slot is 4 bytes)
        let frame_no_pad = build_mp3_frame(3, 3, false, 4, 0, false);
        let frame_with_pad = build_mp3_frame(3, 3, false, 4, 0, true);
        assert_eq!(frame_with_pad.len(), frame_no_pad.len() + 4);

        let mut stream = MockStreamReader::new(frame_with_pad.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame_with_pad);
    }

    #[tokio::test]
    async fn test_multiple_consecutive_frames() {
        let frame1 = build_mp3_frame(3, 1, false, 9, 0, false);
        let frame2 = build_mp3_frame(3, 1, false, 14, 0, false);

        let mut combined = frame1.clone();
        combined.extend_from_slice(&frame2);

        let mut stream = MockStreamReader::new(combined);
        let mut reader = Mp3Reader::new(&mut stream);

        assert_eq!(reader.read().await.unwrap(), frame1);
        assert_eq!(reader.read().await.unwrap(), frame2);
    }

    #[tokio::test]
    async fn test_all_mpeg1_layer3_bitrates() {
        // Verify all valid bitrate indices (1-14) produce parseable frames
        for idx in 1..=14u8 {
            let frame = build_mp3_frame(3, 1, false, idx, 0, false);
            let mut stream = MockStreamReader::new(frame.clone());
            let mut reader = Mp3Reader::new(&mut stream);
            let result = reader.read().await.unwrap();
            assert_eq!(result, frame, "Failed for bitrate_index {}", idx);
        }
    }

    #[tokio::test]
    async fn test_all_mpeg1_sample_rates() {
        for (idx, _expected_rate) in [(0u8, 44100u32), (1, 48000), (2, 32000)] {
            let frame = build_mp3_frame(3, 1, false, 9, idx, false);
            let mut stream = MockStreamReader::new(frame.clone());
            let mut reader = Mp3Reader::new(&mut stream);
            let result = reader.read().await.unwrap();
            assert_eq!(result, frame, "Failed for sample_rate_index {}", idx);
        }
    }

    // ===========================================================================
    // SYNC WORD
    // ===========================================================================

    #[tokio::test]
    async fn test_bad_sync_word_byte0() {
        let mut header = build_valid_header();
        header[0] = 0xFE; // corrupt upper sync bits
        // With only 4 invalid bytes, scanning will fail with "unexpected end of stream"
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bad_sync_word_byte1_upper_bits() {
        let mut header = build_valid_header();
        header[1] &= 0x1F; // corrupt sync bits in byte 1
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_zero_bytes() {
        let header = vec![0x00; 4];
        let mut stream = MockStreamReader::new(header);
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sync_word_scanning_junk_before_valid_header() {
        // Stream starts with 3 bytes of junk, then a valid frame
        let frame = build_valid_frame();
        let mut data = vec![0xAB, 0xCD, 0xEF]; // junk bytes
        data.extend_from_slice(&frame);

        let mut stream = MockStreamReader::new(data);
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_sync_word_scanning_false_sync() {
        // A false sync word (0xFF 0xE0 but invalid header) followed by valid frame.
        // 0xFF 0xE0 = sync word matches, but layer=0 (reserved) → invalid header
        let frame = build_valid_frame();
        let false_sync = [0xFF, 0xE0, 0x00, 0x00];
        let mut data = false_sync.to_vec();
        data.extend_from_slice(&frame);

        let mut stream = MockStreamReader::new(data);
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_sync_word_scanning_multiple_false_syncs() {
        // Multiple false sync words before a valid frame
        let frame = build_valid_frame();
        let false_sync1 = [0xFF, 0xE0, 0x00, 0x00]; // layer=0 (reserved)
        let false_sync2 = [0xFF, 0xE2, 0xF0, 0x00]; // bitrate_index=15 (bad)
        let mut data = false_sync1.to_vec();
        data.extend_from_slice(&false_sync2);
        data.extend_from_slice(&frame);

        let mut stream = MockStreamReader::new(data);
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_sync_word_scanning_id3_like_prefix() {
        // Simulate a small ID3-like prefix ("ID3" magic + some bytes)
        let frame = build_valid_frame();
        let id3_prefix = b"ID3\x03\x00\x00\x00\x00\x10"; // "ID3" + version + flags + size
        let mut data = id3_prefix.to_vec();
        data.extend_from_slice(&frame);

        let mut stream = MockStreamReader::new(data);
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    // ===========================================================================
    // INVALID HEADERS (must be rejected, scanner will exhaust the stream)
    // ===========================================================================

    #[tokio::test]
    async fn test_reserved_mpeg_version() {
        // version = 01 (reserved) — scanner skips this and fails on empty stream
        let header = build_mp3_header(1, 1, false, 9, 0, false);
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reserved_layer() {
        // layer = 00 (reserved)
        let header = build_mp3_header(3, 0, false, 9, 0, false);
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bad_bitrate_index() {
        // bitrate_index = 15 (bad/forbidden)
        let header = build_mp3_header(3, 1, false, 15, 0, false);
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_free_bitrate_index() {
        // bitrate_index = 0 (free format — frame size indeterminable)
        let header = build_mp3_header(3, 1, false, 0, 0, false);
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reserved_sampling_rate() {
        // sampling_rate_index = 3 (reserved)
        let header = build_mp3_header(3, 1, false, 9, 3, false);
        let mut stream = MockStreamReader::new(header.to_vec());
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_header_followed_by_valid_frame() {
        // A frame with reserved version is skipped, scanner finds the next valid frame
        let invalid_header = build_mp3_header(1, 1, false, 9, 0, false);
        let frame = build_valid_frame();
        let mut data = invalid_header.to_vec();
        data.extend_from_slice(&frame);

        let mut stream = MockStreamReader::new(data);
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    // ===========================================================================
    // FRAME SIZE CALCULATION
    // ===========================================================================

    #[tokio::test]
    async fn test_frame_size_mpeg1_layer3_128kbps_44100hz() {
        let header_bytes = build_mp3_header(3, 1, false, 9, 0, false);
        let header = Mp3Header { buf: header_bytes };
        assert_eq!(header.frame_size(), 417);
    }

    #[tokio::test]
    async fn test_frame_size_mpeg1_layer3_128kbps_48000hz() {
        let header_bytes = build_mp3_header(3, 1, false, 9, 1, false);
        let header = Mp3Header { buf: header_bytes };
        // 144 * 128000 / 48000 = 384
        assert_eq!(header.frame_size(), 384);
    }

    #[tokio::test]
    async fn test_frame_size_mpeg1_layer3_128kbps_32000hz() {
        let header_bytes = build_mp3_header(3, 1, false, 9, 2, false);
        let header = Mp3Header { buf: header_bytes };
        // 144 * 128000 / 32000 = 576
        assert_eq!(header.frame_size(), 576);
    }

    #[tokio::test]
    async fn test_frame_size_mpeg1_layer2_384kbps_32000hz() {
        // bitrate_index=14 → 384kbps for Layer II
        let header_bytes = build_mp3_header(3, 2, false, 14, 2, false);
        let header = Mp3Header { buf: header_bytes };
        // 144 * 384000 / 32000 = 1728
        assert_eq!(header.frame_size(), 1728);
    }

    #[tokio::test]
    async fn test_frame_size_mpeg1_layer1_128kbps_44100hz() {
        let header_bytes = build_mp3_header(3, 3, false, 4, 0, false);
        let header = Mp3Header { buf: header_bytes };
        // (12 * 128000 / 44100) * 4 = 34 * 4 = 136
        assert_eq!(header.frame_size(), 136);
    }

    #[tokio::test]
    async fn test_frame_size_mpeg2_layer3_64kbps_22050hz() {
        // bitrate_index=8 → 64kbps for MPEG2 Layer III
        let header_bytes = build_mp3_header(2, 1, false, 8, 0, false);
        let header = Mp3Header { buf: header_bytes };
        // 72 * 64000 / 22050 = 208
        assert_eq!(header.frame_size(), 208);
    }

    #[tokio::test]
    async fn test_frame_size_mpeg2_5_layer3_64kbps_11025hz() {
        // bitrate_index=8 → 64kbps for MPEG2.5 Layer III
        let header_bytes = build_mp3_header(0, 1, false, 8, 0, false);
        let header = Mp3Header { buf: header_bytes };
        // 72 * 64000 / 11025 = 417
        assert_eq!(header.frame_size(), 417);
    }

    #[tokio::test]
    async fn test_frame_size_with_padding_layer3() {
        let header_no_pad = build_mp3_header(3, 1, false, 9, 0, false);
        let header_with_pad = build_mp3_header(3, 1, false, 9, 0, true);
        let h1 = Mp3Header { buf: header_no_pad };
        let h2 = Mp3Header { buf: header_with_pad };
        assert_eq!(h2.frame_size(), h1.frame_size() + 1);
    }

    #[tokio::test]
    async fn test_frame_size_with_padding_layer1() {
        let header_no_pad = build_mp3_header(3, 3, false, 4, 0, false);
        let header_with_pad = build_mp3_header(3, 3, false, 4, 0, true);
        let h1 = Mp3Header { buf: header_no_pad };
        let h2 = Mp3Header { buf: header_with_pad };
        // Layer I padding adds 4 bytes (each slot is 4 bytes)
        assert_eq!(h2.frame_size(), h1.frame_size() + 4);
    }

    // ===========================================================================
    // I/O ERRORS
    // ===========================================================================

    #[tokio::test]
    async fn test_io_error_on_header_read() {
        let mut stream = ErrorMockStreamReader;
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }

    #[tokio::test]
    async fn test_io_error_on_payload_read() {
        // Header is valid but no payload data available
        let header = build_valid_header();
        let mut stream = MockStreamReader::new(header.to_vec());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_reader_io_error() {
        let mut stream = ErrorMockStreamReader;
        let mut reader = Mp3Reader::new(&mut stream);
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
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_partial_header_only_3_bytes() {
        let mut stream = MockStreamReader::new(vec![0xFF, 0xFB, 0x90]);
        let result = Mp3Header::read(&mut stream).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_short_reads_byte_by_byte() {
        let frame = build_valid_frame();
        let mut stream = ShortReadMockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_short_reads_mpeg2() {
        // bitrate_index=8 → 64kbps for MPEG2 Layer III
        let frame = build_mp3_frame(2, 1, false, 8, 0, false);
        let mut stream = ShortReadMockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    #[tokio::test]
    async fn test_short_reads_with_crc() {
        let frame = build_mp3_frame(3, 1, true, 9, 0, false);
        let mut stream = ShortReadMockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result, frame);
    }

    // ===========================================================================
    // FIELD ACCESSORS
    // ===========================================================================

    #[tokio::test]
    async fn test_mpeg_version_accessor() {
        for version_raw in [0u8, 2, 3] {
            let header = build_mp3_header(version_raw, 1, false, 9, 0, false);
            let h = Mp3Header { buf: header };
            assert_eq!(h.mpeg_version(), version_raw);
        }
    }

    #[tokio::test]
    async fn test_layer_accessor() {
        for layer_raw in [1u8, 2, 3] {
            // Layer I needs a valid bitrate_index; use appropriate ones per layer
            let bitrate_idx = if layer_raw == 3 { 4 } else { 9 };
            let header = build_mp3_header(3, layer_raw, false, bitrate_idx, 0, false);
            let h = Mp3Header { buf: header };
            assert_eq!(h.layer(), layer_raw);
        }
    }

    #[tokio::test]
    async fn test_bitrate_accessor() {
        let header = build_mp3_header(3, 1, false, 9, 0, false);
        let h = Mp3Header { buf: header };
        assert_eq!(h.bitrate_kbps(), 128);
    }

    #[tokio::test]
    async fn test_sampling_rate_accessor() {
        let header = build_mp3_header(3, 1, false, 9, 0, false);
        let h = Mp3Header { buf: header };
        assert_eq!(h.sampling_rate_hz(), 44100);
    }

    #[tokio::test]
    async fn test_bitrate_accessor_all_mpeg1_layer3() {
        let expected = [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0];
        for (idx, &expected_kbps) in expected.iter().enumerate() {
            let header = build_mp3_header(3, 1, false, idx as u8, 0, false);
            let h = Mp3Header { buf: header };
            assert_eq!(h.bitrate_kbps(), expected_kbps, "bitrate_index={}", idx);
        }
    }

    #[tokio::test]
    async fn test_sampling_rate_accessor_all_mpeg1() {
        let expected = [44100u32, 48000, 32000, 0];
        for (idx, &expected_hz) in expected.iter().enumerate() {
            let header = build_mp3_header(3, 1, false, 9, idx as u8, false);
            let h = Mp3Header { buf: header };
            assert_eq!(h.sampling_rate_hz(), expected_hz, "sample_rate_index={}", idx);
        }
    }

    // ===========================================================================
    // INFORMATIONAL BITS (must not cause rejection)
    // ===========================================================================

    #[tokio::test]
    async fn test_channel_modes() {
        // Channel mode bits are in byte 3 and should not affect frame reading
        for mode in 0..=3u8 {
            let mut header = build_valid_header();
            header[3] = (mode << 6) | (header[3] & 0x3F);
            let frame_size = {
                let h = Mp3Header { buf: header };
                h.frame_size()
            };
            let mut frame = vec![0xAA; frame_size];
            frame[..4].copy_from_slice(&header);
            let mut stream = MockStreamReader::new(frame.clone());
            let mut reader = Mp3Reader::new(&mut stream);
            let result = reader.read().await.unwrap();
            assert_eq!(result.len(), frame_size, "Channel mode {} should parse", mode);
        }
    }

    #[tokio::test]
    async fn test_informational_bits_set() {
        // Set all informational bits: private=1, channel mode, mode ext, copyright, original, emphasis
        let mut header = build_valid_header();
        header[2] |= 0b0000_0001; // private bit
        header[3] = 0xFF; // all informational bits set
        let frame_size = {
            let h = Mp3Header { buf: header };
            h.frame_size()
        };
        let mut frame = vec![0xAA; frame_size];
        frame[..4].copy_from_slice(&header);
        let mut stream = MockStreamReader::new(frame.clone());
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await.unwrap();
        assert_eq!(result.len(), frame_size);
    }

    #[tokio::test]
    async fn test_protection_bit_does_not_affect_frame_size() {
        let header_no_crc = build_mp3_header(3, 1, false, 9, 0, false);
        let header_with_crc = build_mp3_header(3, 1, true, 9, 0, false);
        let h1 = Mp3Header { buf: header_no_crc };
        let h2 = Mp3Header { buf: header_with_crc };
        // Frame size is the same regardless of CRC — CRC is part of the payload
        assert_eq!(h1.frame_size(), h2.frame_size());
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
