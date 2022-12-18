use std::path::Path;

use async_compression::tokio::bufread::GzipDecoder;
use futures::pin_mut;
use pin_project::pin_project;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncRead, BufReader},
};

/// A wrapper around [GzDecoder] which reads compressed data from a [tokio::fs::File]
/// to take advantage of non-blocking file IO. Implements [AsyncRead]
#[pin_project]
#[derive(Debug)]
pub struct GzDecoderAsync {
    #[pin]
    dec: GzipDecoder<BufReader<File>>,
}

impl GzDecoderAsync {
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let f = File::open(path).await.unwrap();
        let dec = GzipDecoder::new(BufReader::with_capacity(1024 * 1024, f));
        Self { dec }
    }
}

impl AsyncRead for GzDecoderAsync {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let dec = self.project().dec;
        dec.poll_read(cx, buf)
    }
}
