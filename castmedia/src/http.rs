use std::net::SocketAddr;
use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::TcpStream
};
use tokio_native_tls::native_tls::TlsConnector;
use url::Url;

use crate::{server::{Server, Stream}, utils::get_header};

pub struct HttpClient<'a> {
    stream: Stream,
    host: String,
    port: u16,
    path: &'a str,
    headers: Vec<&'a str>,
    http_max_len: usize
}

impl<'a> HttpClient<'a> {
    pub async fn connect(url: &Url, path: &'a str, http_max_len: usize) -> Result<Self> {
        // Host and port should have been verified before
        let host = url.host()
            .expect("Should be able to fetch master host")
            .to_string();
        let port = url.port()
            .expect("Should be able to fetch master port");
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let stream = if url.scheme().eq("https") {
            let cx = tokio_native_tls::TlsConnector::from(TlsConnector::builder().build()?);
            Stream(Box::new(BufStream::new(cx.connect(&host, stream).await?)))
        } else {
            Stream(Box::new(BufStream::new(stream)))
        };

        Ok(Self {
            stream,
            host,
            port,
            path,
            headers: Vec::new(),
            http_max_len
        })
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.stream.peer_addr()
    }

    pub fn add_header(&mut self, header: &'a str) {
        self.headers.push(header);
    }

    pub async fn get(mut self) -> Result<ResponseReader> {
        self.stream.write_all(format!("GET {} HTTP/1.1\r\n\
Host: {}:{}\r\n\
User-Agent: {}\r\n",
            self.path,
            self.host,
            self.port,
            crate::config::SERVER_ID
        ).as_bytes()).await?;
        for header in self.headers {
            self.stream.write_all(header.as_bytes()).await?;
            self.stream.write_all(b"\r\n").await?;
        }
        self.stream.write_all(b"Connection: close\r\n\r\n").await?;
        self.stream.flush().await?;

        Ok(ResponseReader::new(self.stream, self.http_max_len))
    }
}

pub struct ResponseReader {
    stream: Stream,
    http_max_len: usize
}

impl ResponseReader {
    pub fn new(stream: Stream, http_max_len: usize) -> Self {
        Self { stream, http_max_len }
    }

    /// Return inner stream consuming self
    pub fn get_inner_stream(self) -> Stream {
        self.stream
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
                    if buf.len() >= 4 && buf[buf.len()-4..].eq(b"\r\n\r\n") {
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

#[derive(Default)]
pub struct ChunkedResponseReader {
    bytes_left: usize,
    reader: [u8; 1]
}

impl ChunkedResponseReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn read_exact(&mut self, stream: &mut Stream, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut r = 0;
        loop {
            r += self.read(stream, buf).await?;
            if r == buf.len() {
                return Ok(r);
            }
        }
    }

    pub async fn read(&mut self, stream: &mut Stream, buf: &mut [u8]) -> std::io::Result<usize> {
        // We first need to read chunk length that is encoded as hex followed by \r\n
        if self.bytes_left == 0 {
            let mut hex_len = Vec::new();
            loop {
                match stream.read(&mut self.reader).await {
                    Ok(_) => {
                        hex_len.push(self.reader[0]);
                        // Avoiding a ddos here
                        if hex_len.len() > 12 {
                            return std::io::Result::Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "Peer trying to send a big size chunk"))
                        } else if hex_len.ends_with(&[b'\r', b'\n']) {
                            break;
                        }
                    },
                    Err(e) => return Err(e)
                }
            }

            self.bytes_left = match std::str::from_utf8(&hex_len[..hex_len.len()-2]) {
                Ok(v) => match usize::from_str_radix(v, 16) {
                    Ok(v) => v,
                    Err(_) => return std::io::Result::Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "Chunk length is not a valid number"))
                },
                Err(_) => return std::io::Result::Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "Chunk length is not a valid string"))
            };

            // if we get 0, this means it's the end
            if self.bytes_left == 0 {
                return Ok(0)
            }
        }

        // Now we read actual chunk
        // We make sure we are not reading more than bytes_left
        let read_len = if buf.len() > self.bytes_left {
            self.bytes_left
        } else {
            buf.len()
        };

        match stream.read_exact(&mut buf[..read_len]).await {
            Ok(r) => {
                self.bytes_left -= r;
                if self.bytes_left == 0 {
                    // reading crlf at end
                    stream.read_u16().await?;
                }
                Ok(r)
            },
            Err(e) => Err(e)
        }
    }
}
