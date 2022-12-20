use std::{
    collections::{hash_map::DefaultHasher, HashMap, VecDeque},
    fmt::Debug,
    fs::File,
    hash::Hash,
    io::Write,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use bimap::BiMap;
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
use tokio_scoped::scoped;
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

/// Uniquely represents a JsonLinesWriteStream within a given JsonLinesWriteStreamPool
#[derive(Hash, Clone, Copy, PartialEq, Eq)]
struct WriteStreamId(u16);

impl Debug for WriteStreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // f.debug_tuple("WriteStreamId").field(&self.0).finish()
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for WriteStreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Default)]
pub struct JsonLinesWriteStreamPool {
    // /// The pool of write streams
    // pool: HashMap<PathBuf, JsonLinesWriteStream>,
    /// The pool of all active write streams. Guaranteed to cover the same set of streams as `priority_queue`
    pool: HashMap<WriteStreamId, JsonLinesWriteStream>,
    /// A mapping between paths and stream ids
    ///
    /// Values in this map are not removed when write streams are removed
    id_map: BiMap<PathBuf, WriteStreamId>,
    /// A queue representing which stream should be soonest removed
    ///
    /// This is an `LRU` (Least Recently Used) priority queue.
    /// The backmost is least recently used, the frontmost is most recently used
    priority_queue: VecDeque<WriteStreamId>,
}

impl JsonLinesWriteStreamPool {
    /// The maximum number of open files which will be allowed by the pool
    const MAX_OPEN_FILES: usize = 100;
    fn get_id(&self, path: &PathBuf) -> Option<WriteStreamId> {
        self.id_map.get_by_left(path).copied()
    }
    fn get_id_or_generate(&mut self, path: &PathBuf) -> WriteStreamId {
        if let Some(id) = self.get_id(path) {
            id
        } else {
            //Generate a new unique id randomly
            let mut id = WriteStreamId(rand::random());

            while self.id_map.contains_right(&id) {
                id.0 = rand::random();
            }

            //Update id_map
            self.id_map.insert(path.clone(), id);

            //Return the id
            id
        }
    }
    // /// Safely flushes and closes the given JsonLinesWriteStream and updates `self.pool`possible
    // ///
    // /// DOES NOT update `priority_queue`
    // async fn close(&mut self, id: WriteStreamId) {
    //     //
    // }
    /// Closes the least recently used WriteStream in the pool and updates `pool` and `priority_queue` accordingly
    ///
    /// Returns `None` if the queue was empty, otherwise returns the id of the stream that was closed
    async fn close_least_recently_used(&mut self) -> Option<WriteStreamId> {
        let id = self.priority_queue.pop_back()?;

        //Flush and close down the file and remove from `pool`
        let mut stream = self.pool.remove(&id)?;
        stream.flush().await;
        // stream.finish().await;

        Some(id)
    }

    /// Safely inserts a file stream into the pool
    ///
    /// DOES NOT update `priority_queue` or insert the new stream into it
    async fn insert<'a>(&'a mut self, path: &PathBuf) -> &'a mut JsonLinesWriteStream {
        //Get the id. Note that it is possible and expected for streams which have been forgotten already to remain in `id_map`
        let id = self.get_id_or_generate(path);

        //If the stream limit is currently reached, first remove the least recently used stream
        if self.priority_queue.len() >= Self::MAX_OPEN_FILES {
            //TODO:
            //Run this closing concurrently with the write stream creation (possible using tokio_scoped)
            self.close_least_recently_used().await;
        }

        //Create the stream
        let stream = JsonLinesWriteStream::new(path).await;
        self.pool.insert(id, stream);

        //Return a reference to the stream which is now in `pool`
        self.pool.get_mut(&id).unwrap()
    }
    async fn get_or_insert<'a>(&'a mut self, path: &PathBuf) -> &'a mut JsonLinesWriteStream {
        let id = self.get_id_or_generate(path);
        //Make sure the entry is populated
        match self.pool.entry(id) {
            std::collections::hash_map::Entry::Occupied(_) => self.pool.get_mut(&id).unwrap(),
            std::collections::hash_map::Entry::Vacant(_) => {
                // println!(
                //     "New file to be opened. Now there are {}={} open files",
                //     self.pool.len(),
                //     self.priority_queue.len(),
                // );
                return self.insert(path).await;
            }
        }
    }

    pub async fn write(&mut self, path: &PathBuf, bytes: Bytes) {
        let id = self.get_id_or_generate(path);
        //Take `id` in `priority_queue` and put it at the front of the queue, since it has been most recently used

        //Remove any previous entries for `id` in `priority_queue`
        for i in (0..self.priority_queue.len()).rev() {
            if self.priority_queue[i] == id {
                self.priority_queue.remove(i);
                break;
            }
        }

        //Regardless of whether or not `id` was previously in `priority_queue`, it will now be the frontmost item
        self.priority_queue.push_front(id);

        self.get_or_insert(path).await.write_bytes(&bytes).await;
    }

    pub async fn write_line(&mut self, line: Line) {
        let Line {
            target_file: path,
            text,
        } = line;

        let id = self.get_id_or_generate(&path);

        //Take `id` in `priority_queue` and put it at the front of the queue, since it has been most recently used

        //Remove any previous entries for `id` in `priority_queue`
        for i in (0..self.priority_queue.len()).rev() {
            if self.priority_queue[i] == id {
                self.priority_queue.remove(i);
                break;
            }
        }

        //Regardless of whether or not `id` was previously in `priority_queue`, it will now be the frontmost item
        self.priority_queue.push_front(id);
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
    // buffer: VecDeque<char>,
    /// The buffer of chars used in `next_line` for when a newline is found partway through a reading.
    ///
    /// In essence, stores some of the first values of the next line which has yet to be read
    ///
    /// Note that the next characters are only described by the slice `buf[buf_start_index..buf.len()]`
    buf: [u8; Self::BUFFER_SIZE],
    /// The range of valid bytes in `buf`
    buf_valid_range: Range<usize>,
    /// `true` if and only if the input stream's EOF has been reached
    finished: bool,
}

impl JsonLinesReadStream {
    const BUFFER_SIZE: usize = 64;
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            input_stream: GzDecoderAsync::new(path).await,
            // buffer: VecDeque::with_capacity(Self::BUFFER_SIZE),
            buf: [0; Self::BUFFER_SIZE],
            buf_valid_range: 0..0,
            finished: false,
        }
    }

    /// Gets the next JSON line (which contains a whole object)
    pub async fn next_line(&mut self) -> Result<String, tokio::io::Error> {
        if self.finished {
            return Ok(String::new());
        }

        // Allocate the next line with the knowledge of the minimum number of bytes which will be pushed into it
        // (that is, all the remaining buffered bytes)
        let mut line = String::with_capacity(self.buf_valid_range.len());

        // //Before reading any new values, read from `self.buffer`
        for i in self.buf_valid_range.clone() {
            let c = self.buf[i] as char;
            if c == '\n' {
                //Still need to update `buf_start_index` in case multiple lines were read into the same buffer
                self.buf_valid_range.start = i + 1;
                return Ok(line);
            }
            line.push(c);
        }

        'outer: loop {
            let chars_read = self.input_stream.read(&mut self.buf).await?;

            //EOF is signified by `Ok(0)` and can happen after multiple reads in a loop
            //So, make sure to still return the final line
            if chars_read == 0 {
                println!("EOF reached");
                // println!(
                //     "Final buf: {:?}",
                //     self.buf.iter().map(|n| *n as char).collect::<Vec<_>>()
                // );
                self.finished = true;
                self.buf_valid_range = 0..0;
                return Ok(line);
            }

            for i in 0..chars_read {
                let c = self.buf[i] as char;
                if c == '\n' {
                    // //Write the rest of the characters to `self.buffer`
                    //Note the `+ 1` which exists so that the current character (a newline) is not read

                    // println!("Finished reading line: {line}");
                    self.buf_valid_range = (i + 1)..(chars_read);
                    break 'outer;
                }
                line.push(c);
            }
        }

        if false {
            println!("line: {line}\n");
        }
        Ok(line)
    }
    // /// Gets the next JSON line (which contains a whole object)
    // pub async fn next_line(&mut self) -> Result<String, tokio::io::Error> {
    //     let mut line = String::new();
    //     let buf = &mut [0; Self::BUFFER_SIZE][..];

    //     // //Before reading any new values, read from `self.buffer`
    //     // for c in self.buf[self.buf_start_index..self.buf.len()].iter() {
    //     //     let c = *c as char;
    //     //     if c == '\n' {
    //     //         return Ok(line);
    //     //     }
    //     //     line.push(c);
    //     // }
    //     while let Some(c) = self.buffer.pop_back() {
    //         if c == '\n' {
    //             return Ok(line);
    //         }
    //         line.push(c);
    //     }

    //     'outer: loop {
    //         let chars_read = self.input_stream.read(buf).await?;
    //         //Generate a buffer which is the same as `buf` except that it only counts characters that were actually read
    //         let buf = &buf[0..chars_read];

    //         //EOF is signified by `Ok(0)`
    //         if chars_read == 0 {
    //             println!("EOF reached");
    //             return Ok(String::new());
    //         }

    //         for (i, c) in buf.iter().enumerate() {
    //             let c = *c as char;
    //             if c == '\n' {
    //                 // //Write the rest of the characters to `self.buffer`
    //                 // //Note the `+ 1` which exists so that the current character (a newline) is not read
    //                 for c in buf[i + 1..chars_read].iter() {
    //                     let c = *c as char;
    //                     self.buffer.push_front(c);
    //                 }
    //                 // println!("Finished reading line: {line}");
    //                 // self.buf_start_index = i + 1;
    //                 break 'outer;
    //             }
    //             line.push(c);
    //         }
    //     }

    //     if false {
    //         println!("line: {line}\n");
    //     }
    //     Ok(line)
    // }
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
