use anyhow::Result;
use tokio::io::AsyncReadExt;

use crate::{server::Stream, utils::get_header};


pub struct ResponseReader<'a> {
    stream: &'a mut Stream,
    http_max_len: usize
}

impl<'a> ResponseReader<'a> {
    pub fn new(stream: &'a mut Stream, http_max_len: usize) -> Self {
        Self { stream, http_max_len }
    }

    /// Read header only and return it's buffer
    pub async fn read_headers(&mut self) -> Result<Vec<u8>> {
        let mut buf  = Vec::new();
		let mut byte = [ 0; 1 ];
		loop {
			match self.stream.read(&mut byte).await {
				Ok(_) => {
					buf.extend_from_slice(&byte);
					// checking if double crlf is in header
					if buf.windows(4).any(|window| window == b"\r\n\r\n") { // end of header
						break;
					} else if buf.len() > self.http_max_len {
						// Stop any potential attack
						return Err(anyhow::Error::msg("long header"))
					}
				},
				Err(e) => return Err(e.into())
			}
		};

        Ok(buf)
    }

    /// Read content-length response with predicted content type
    pub async fn read_to_end(&mut self, content_type: &str) -> Result<Vec<u8>> {
        // We start off by reading raw server response then we parse it
        let headers_buf = self.read_headers().await?;
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut resp    = httparse::Response::new(&mut headers);

        match resp.parse(&headers_buf) {
            Ok(httparse::Status::Complete(_)) => {},
            Ok(httparse::Status::Partial) => return Err(anyhow::Error::msg("Incomplete response")),
            Err(e) => return Err(e.into())
        };

        // Now we do sanity checks on response
        match resp.code {
            Some(code) => if code != 200 {
                return Err(anyhow::Error::msg("Status code is not 200"));
            },
            _ => return Err(anyhow::Error::msg("Received unexpected response"))
        }

        // Checking content-type of body
        match get_header("content-type", resp.headers) {
            Some(header) => {
                if !header.starts_with(content_type.as_bytes()) {
                    return Err(anyhow::Error::msg(format!("Response must be of type {}", content_type)));
                }
            },
            None => {
                return Err(anyhow::Error::msg("Missing content-type header"));
            }
        }

        // Checking if body length match what is in content-length
        let len = match get_header("content-length", resp.headers) {
            Some(header) => {
                std::str::from_utf8(header)?
                    .parse::<usize>()?
            },
            None => {
                return Err(anyhow::Error::msg("Missing content-length header"));
            }
        };

        if headers_buf.len() + len > 5000 {
            return Err(anyhow::Error::msg("Received large http response"));
        }

        let mut buf = vec![0 ; len];
        self.stream.read_exact(&mut buf[..]).await?;

        Ok(buf)
    }
}
