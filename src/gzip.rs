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

use crate::{dec::GzDecoderAsync, enc::GzEncoderAsync};
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
        println!("new JsonLinesWriteStream: {path:?}");

        Self {
            encode_stream: GzEncoderAsync::new(path).await,
        }

        // let path: &Path = path.as_ref();
        // println!("new JsonLinesWriteStream: {path:?}");

        // let f = tokio::fs::File::create(path).await.unwrap();

        // let f = f.into_std().await;
        // // let f = File::create(path).await.unwrap();

        // // let (send, recv) = unbounded_channel();

        // // let recv = UnboundedReceiverStream::new(recv);
        // // let encode_stream = {
        // //     let encode_stream = StreamReader::new(recv);
        // //     GzipEncoder::new(BufReader::new(encode_stream))
        // // };

        // // Self {
        // //     input_stream: send,
        // //     encode_stream,
        // //     output_file: f,
        // // }
        // Self {
        //     encode_stream: GzEncoder::new(f, Compression::new(5)),
        // }
    }
    pub async fn write_bytes(&mut self, buf: &[u8]) {
        self.encode_stream.write(buf).await.unwrap();
        // self.encode_stream.write_all(buf);
    }
    pub async fn flush(&mut self) {
        self.encode_stream.flush().await;
        // self.encode_stream.flush();
    }
    pub async fn finish(self) {
        self.encode_stream.finish().await;
        // self.encode_stream.finish();
    }
    // pub async fn queue_bytes(&self, bytes: Bytes) {
    //     self.input_stream.send(Ok(bytes)).unwrap();
    // }
    // pub async fn flush_bytes_to_file(&mut self) {
    //     //Mark the end of the file
    //     self.input_stream
    //         .send(Err(std::io::ErrorKind::UnexpectedEof.into()))
    //         .unwrap();

    //     let mut buf = Vec::new();

    //     loop {
    //         let b = self.encode_stream.read_u8().await;
    //         println!("{b:?}");
    //         match b {
    //             Ok(v) => buf.push(v),
    //             Err(e) => match e.kind() {
    //                 std::io::ErrorKind::UnexpectedEof => break,
    //                 _ => todo!(),
    //             },
    //         }
    //     }

    //     println!("buf: {buf:?}");
    //     self.output_file.write_all(&buf).await.unwrap();
    // }
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
        // self.input_stream.
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

            // let c = match self.input_stream.read_u8().await {
            //     Ok(c) => c,
            //     Err(e) => match e.kind() {
            //         std::io::ErrorKind::UnexpectedEof => {
            //             println!("EOF reached");
            //             return Ok(String::new());
            //         }
            //         _ => return Err(e),
            //     },
            // } as char;
            // if c == '\n' {
            //     break;
            // }

            // line.push(c);
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

    // let f = tokio::fs::File::open(path).await?.into_std().await;
    // let dec = GzDecoder::new(std::io::BufReader::new(f));

    // let f_read = BufReader::with_capacity(READER_BUF_SIZE, f);
    // let dec = GzDecoder::new(f_read);

    // let f_write = BufWriter::with_capacity(
    //     WRITER_BUF_SIZE,
    //     File::create(path.parent().unwrap().join("out/out.json.gz")).await?,
    // );
    // let enc = GzipEncoder::new(f_write);

    // let mut s = String::new();
    // dec.read_to_string(&mut s).await?;
    // println!("Decoded:\n{}", s);
    Ok(JsonLinesReadStream::new(path).await)
    // Ok((JsonLinesReadStream::new(dec), JsonLineWriteStream::new(enc)))
}
