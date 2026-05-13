use anyhow::Result;

use tokio::io::{AsyncRead, AsyncReadExt};
use crate::audio::AudioReader;

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

    /// Attempt to parse a valid MP3 header from a 4-byte buffer.
    fn from_buf(buf: [u8; 4]) -> Option<Self> {
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
                    return Some(header);
                }
            }
        }
        None
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

/// State machine for reading MP3 frames.
enum Mp3ReadState {
    /// Scanning for a valid MP3 frame header using a 4-byte sliding window.
    ///
    /// The `window` buffer holds up to 4 bytes of the candidate header.
    /// `filled` tracks how many bytes are valid (0..4). When `filled == 4`,
    /// we validate the header. If invalid, we shift the window left by 1
    /// and set `filled = 3` to read one more byte and try again.
    HeaderSearch {
        window: [u8; 4],
        filled: usize,
    },
    /// Valid header found, reading the remaining frame payload.
    ///
    /// `frame` is pre-allocated to `frame_size` bytes with the 4-byte header
    /// already filled in. `filled` tracks how many total bytes have been
    /// written (starts at 4, goes up to `frame_size`).
    Payload {
        frame: Vec<u8>,
        frame_size: usize,
        filled: usize,
    },
}

pub struct Mp3Reader<'a, R: AsyncRead + Unpin> {
    stream: &'a mut R,
    state: Mp3ReadState,
}

impl<'a, R: AsyncRead + Unpin> Mp3Reader<'a, R> {
    pub fn new(stream: &'a mut R) -> Self {
        Self {
            stream,
            state: Mp3ReadState::HeaderSearch {
                window: [0u8; 4],
                filled: 0,
            },
        }
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Unpin + Send> AudioReader for Mp3Reader<'_, R> {
    async fn read(&mut self) -> Result<Vec<u8>> {
        loop {
            match &mut self.state {
                Mp3ReadState::HeaderSearch { window, filled } => {
                    if *filled < Mp3Header::SIZE {
                        // Read bytes into the sliding window until we have 4.
                        // `stream.read()` is cancel-safe: if dropped, no bytes
                        // are consumed. We update `filled` synchronously after
                        // the read returns, so progress is always persisted.
                        match self.stream.read(&mut window[*filled..Mp3Header::SIZE]).await {
                            Ok(0) => return Err(anyhow::Error::msg("MP3 unexpected end of stream")),
                            Ok(n) => *filled += n,
                            Err(_) => return Err(anyhow::Error::msg("MP3 IO error")),
                        }
                        continue;
                    }

                    // Have 4 bytes — check if they form a valid header
                    if let Some(header) = Mp3Header::from_buf(*window) {
                        let frame_size = header.frame_size();
                        let mut frame = vec![0u8; frame_size];
                        frame[..Mp3Header::SIZE].copy_from_slice(window);
                        // Transition to Payload state synchronously.
                        // If cancelled at the next .await, this state is preserved.
                        self.state = Mp3ReadState::Payload {
                            frame,
                            frame_size,
                            filled: Mp3Header::SIZE,
                        };
                        continue;
                    }

                    // Invalid header — shift window left by 1 byte and try again.
                    // This discards the leftmost byte and we'll read one new byte.
                    // No .await here, so this is synchronous and cancel-safe.
                    window[0] = window[1];
                    window[1] = window[2];
                    window[2] = window[3];
                    *filled = 3;
                }
                Mp3ReadState::Payload { frame, frame_size, filled } => {
                    if *filled < *frame_size {
                        // Read payload bytes. Same cancel-safety guarantees as above:
                        // stream.read() is cancel-safe, and we update `filled`
                        // synchronously after each read.
                        match self.stream.read(&mut frame[*filled..*frame_size]).await {
                            Ok(0) => return Err(anyhow::Error::msg("MP3 unexpected end of stream")),
                            Ok(n) => *filled += n,
                            Err(_) => return Err(anyhow::Error::msg("MP3 IO error")),
                        }
                        continue;
                    }

                    // Frame is complete — return it and reset state for next frame
                    let result = std::mem::take(frame);
                    self.state = Mp3ReadState::HeaderSearch {
                        window: [0u8; 4],
                        filled: 0,
                    };
                    return Ok(result);
                }
            }
        }
    }

    fn into_partial_frame(self: Box<Self>) -> Vec<u8> {
        match self.state {
            Mp3ReadState::HeaderSearch { window, filled } => window[..filled].to_vec(),
            Mp3ReadState::Payload { frame, filled, .. } => frame[..filled].to_vec(),
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
            Self {
                data: Cursor::new(data),
            }
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
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "mock IO error",
            )))
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
        header[0] = 0xFE;
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_bad_sync_word_byte1_upper_bits() {
        let mut header = build_valid_header();
        header[1] &= 0x1F;
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_zero_bytes() {
        let result = Mp3Header::from_buf([0x00; 4]);
        assert!(result.is_none());
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
        let header = build_mp3_header(1, 1, false, 9, 0, false);
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_reserved_layer() {
        let header = build_mp3_header(3, 0, false, 9, 0, false);
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_bad_bitrate_index() {
        let header = build_mp3_header(3, 1, false, 15, 0, false);
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_free_bitrate_index() {
        let header = build_mp3_header(3, 1, false, 0, 0, false);
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_reserved_sampling_rate() {
        let header = build_mp3_header(3, 1, false, 9, 3, false);
        let result = Mp3Header::from_buf(header);
        assert!(result.is_none());
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
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }

    #[tokio::test]
    async fn test_io_error_on_payload_read() {
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
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected end of stream"));
    }

    #[tokio::test]
    async fn test_partial_header_only_3_bytes() {
        let mut stream = MockStreamReader::new(vec![0xFF, 0xFB, 0x90]);
        let mut reader = Mp3Reader::new(&mut stream);
        let result = reader.read().await;
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

}
