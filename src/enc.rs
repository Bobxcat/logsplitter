use std::{
    collections::VecDeque,
    io::{Read, Write},
    path::Path,
    rc::Rc,
    sync::Arc,
};

use bytes::Bytes;
use flate2::{read::GzEncoder, Compression};
// use flate2::write::GzEncoder;
use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt},
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
    f: File,
}

impl GzEncoderAsync {
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let f = File::create(path).await.unwrap();

        // let queue_in = GzEncQueue {
        //     queue: Arc::new(Mutex::new(VecDeque::new())),
        // };

        let enc = GzEncoder::new(VecDeque::new(), Compression::new(5));
        Self { enc, f }
    }
    /// Encodes `buf` into Gzip and appends the encoded bytes to the file
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let buf = block_in_place(|| -> Result<Vec<u8>, std::io::Error> {
            //First, push the buffer to the encoding queue
            self.enc.write(buf)?;

            println!("Encoding Buf:\n    {buf:?}");
            let mut buf = Vec::new();

            //Second, read the encoded output into a buffer
            loop {
                let tmp_buf = &mut [0; 32][..];
                match self.enc.read(tmp_buf) {
                    //Add all read bytes to `buf`, but if 0 were read then break the loop
                    Ok(bytes_read) => {
                        dbg!(bytes_read);
                        if bytes_read == 0 {
                            break;
                        }
                        for i in 0..bytes_read {
                            buf.push(tmp_buf[i]);
                        }
                    }
                    //If an error was encountered, ignore iff it's of kind `Interrupted`
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::Interrupted => continue,
                        _ => return Err(e),
                    },
                }
            }
            println!("    {buf:?}");

            Ok(buf)
        })?;

        //Finally, write the encoded output into the file asyncronously
        self.f.write_all_buf(&mut Bytes::from(buf)).await?;

        Ok(())
    }
    pub async fn flush(&mut self) {
        //
    }
    pub async fn finish(self) {
        //
    }
}
