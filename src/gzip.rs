use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

// use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
use bytes::{Bytes, BytesMut};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use futures::{future::join_all, sink::Buffer, stream::FuturesUnordered, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::ReadDir,
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufStream, BufWriter},
    runtime::Handle,
    sync::{
        mpsc::{channel, unbounded_channel, Receiver, Sender, UnboundedSender},
        Mutex,
    },
    task::{block_in_place, spawn_blocking, JoinSet},
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tokio_util::io::StreamReader;

use crate::{dec::GzDecoderAsync, enc::GzEncoderAsync, Line};
// use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
struct MpscRecvAsync {
    stream: Receiver<u8>,
}

impl AsyncRead for MpscRecvAsync {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll::*;
        match self.get_mut().stream.poll_recv(cx) {
            Ready(Some(dat)) => {
                buf.put_slice(&[dat]);
                Ready(Ok(()))
            }
            Ready(None) => Ready(Err(std::io::ErrorKind::ConnectionAborted.into())),
            Pending => Pending,
        }
    }
}

#[derive(Debug, Default)]
pub struct JsonLinesWriteStreamPool {
    /// The pool of write streams
    pool: HashMap<PathBuf, JsonLinesWriteStream>,
}

impl JsonLinesWriteStreamPool {
    async fn get_or_insert<'a>(&'a mut self, path: &PathBuf) -> &'a mut JsonLinesWriteStream {
        //Make sure the entry is populated
        match self.pool.entry(path.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => (),
            std::collections::hash_map::Entry::Vacant(_) => {
                let s = JsonLinesWriteStream::new(path).await;
                self.pool.insert(path.clone(), s);
            }
        }

        //Get a mutable reference
        self.pool.get_mut(path).unwrap()
    }

    pub async fn write(&mut self, path: &PathBuf, bytes: Bytes) {
        self.get_or_insert(path).await.write_bytes(&bytes).await;
    }

    pub async fn write_line(&mut self, line: Line) {
        let Line {
            target_file: path,
            text,
        } = line;
        self.get_or_insert(&path).await.write_line(text).await;
    }

    pub async fn finish_all(self) {
        for (_path, s) in self.pool {
            s.finish().await;
        }
    }
}

/// Represents a buffered stream which takes inputted bytes (uncompressed) and writes them to a given file (compressed)
#[derive(Debug)]
struct JsonLinesWriteStream {
    encode_stream: GzEncoderAsync,
}

impl JsonLinesWriteStream {
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let path: &Path = path.as_ref();

        Self {
            encode_stream: GzEncoderAsync::new(path, 5).await,
        }
    }
    pub async fn write_bytes(&mut self, buf: &[u8]) {
        self.encode_stream.write(buf).await.unwrap();
    }
    pub async fn write_line(&mut self, mut line: String) {
        line.push('\n');
        self.write_bytes(line.as_bytes()).await;
    }
    pub async fn flush(&mut self) {
        self.encode_stream.flush().await;
    }
    pub async fn finish(self) {
        // self.encode_stream.finish().await;
        todo!()
    }
}

#[derive(Debug)]
pub struct JsonLinesReadStream {
    input_stream: GzDecoderAsync,
}

impl JsonLinesReadStream {
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            input_stream: GzDecoderAsync::new(path).await,
        }
    }
    /// Gets the next JSON line (which contains a whole object)
    pub async fn next_line(&mut self) -> Result<String, tokio::io::Error> {
        let mut line = String::new();
        loop {
            let c = match self.input_stream.read_u8().await {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        println!("EOF reached");
                        return Ok(String::new());
                    }
                    _ => return Err(e),
                },
            } as char;
            if c == '\n' {
                break;
            }

            line.push(c);
        }

        Ok(line)
    }
}

/// The size of the file reader's buffer. Currently 1 MiB
pub const READER_BUF_SIZE: usize = 1024 * 1024;
pub const WRITER_BUF_SIZE: usize = 1024 * 256;

pub async fn open_file<F>(path: F) -> anyhow::Result<JsonLinesReadStream>
where
    F: AsRef<Path>,
{
    let path: &Path = path.as_ref();

    Ok(JsonLinesReadStream::new(path).await)
}
