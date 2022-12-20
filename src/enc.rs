use std::{
    collections::VecDeque,
    io::{Read, Write},
    path::Path,
    rc::Rc,
    sync::Arc,
    thread::{scope, ScopedJoinHandle},
};

use bytes::Bytes;
use flate2::{read::GzEncoder, Compression};
// use flate2::write::GzEncoder;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWrite, AsyncWriteExt},
    runtime::Handle,
    sync::{mpsc, Mutex},
    task::{block_in_place, spawn_blocking},
};

// /// A queue with an internal mutex to allow for a `Read` implementation while being expandable
// #[derive(Debug, Default, Clone)]
// struct GzEncQueue {
//     queue: Arc<Mutex<VecDeque<u8>>>,
// }

// impl std::io::Read for GzEncQueue {
//     fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
//         //This is pretty expensive
//         let mut lock = self.queue.blocking_lock();
//         lock.read(buf)
//     }
// }

// impl std::io::Write for GzEncQueue {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         let mut lock = self.queue.blocking_lock();
//         lock.write(buf)
//     }

//     fn flush(&mut self) -> std::io::Result<()> {
//         let mut lock = self.queue.blocking_lock();
//         lock.flush()
//     }
// }

/// A wrapper around [GzEncoder] which writes its encoded data to a [tokio::fs::File]
/// to take advantage of non-blocking file IO
#[derive(Debug)]
pub struct GzEncoderAsync {
    enc: GzEncoder<VecDeque<u8>>,
    /// The number of bytes queued for writing to the file output. Resets upon calling `flush` along with
    /// and resetting `enc` to a new decoder
    bytes_queued: usize,
    compression_level: u32,
    f: File,
}

impl GzEncoderAsync {
    // The number of bytes (uncompressed) which can be written to each GzEncoder before resetting
    const MAX_BYTES_QUEUED: usize = 1024 * 1024 * 5;
    pub async fn new<P>(path: P, compression_level: u32) -> Self
    where
        P: AsRef<Path>,
    {
        let f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await
            .unwrap();

        let enc = GzEncoder::new(VecDeque::new(), Compression::new(compression_level));
        Self {
            enc,
            f,
            bytes_queued: 0,
            compression_level,
        }
    }
    pub async fn flush(&mut self) {
        //Flush the output to the file
        let mut buf = Vec::with_capacity(self.bytes_queued);
        self.enc.read_to_end(&mut buf).unwrap();
        //Write the file contents asyncrounously
        self.f.write_all(&buf).await.unwrap();
        // let queue_in = GzEncQueue {
        //     queue: Arc::new(Mutex::new(VecDeque::new())),
        // };

        //Reset values, keeping the same file open
        self.bytes_queued = 0;
        self.enc = GzEncoder::new(VecDeque::new(), Compression::new(self.compression_level))
    }
    /// Encodes `buf` into Gzip and appends the encoded bytes to the file
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<()> {
        //Update the number of bytes queued
        self.bytes_queued += buf.len();
        // scope(|sc| -> Vec<ScopedJoinHandle<std::io::Result<()>>> {
        //     sc.spawn(|| -> std::io::Result<()> {
        //     });
        //     vec![]
        // });

        //Push the buffer to the encoding queue
        self.enc.write_all(buf)?;

        //Flush the bytes
        if self.bytes_queued > Self::MAX_BYTES_QUEUED {
            self.flush().await;
        }

        Ok(())
    }
}

impl Drop for GzEncoderAsync {
    fn drop(&mut self) {
        std::thread::scope(|s| {
            s.spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
                runtime.block_on(self.flush());
            });
        });
    }
}
