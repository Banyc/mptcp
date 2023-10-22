use std::{
    io,
    path::{Path, PathBuf},
};

use bytes::BytesMut;
use clap::{Args, Subcommand};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
};

const PAYLOAD_SIZE: usize = 1024;

#[derive(Debug, Clone, Subcommand)]
pub enum FileTransferCommand {
    Push(PushFileArgs),
    Pull(PullFileArgs),
}

impl FileTransferCommand {
    pub async fn perform(
        &self,
        async_read: impl AsyncRead + Unpin,
        async_write: impl AsyncWrite + Unpin,
    ) -> io::Result<usize> {
        match self {
            FileTransferCommand::Push(args) => args.push_file(async_write).await,
            FileTransferCommand::Pull(args) => args.pull_file(async_read).await,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct PushFileArgs {
    pub source_file: PathBuf,
    #[clap(default_value_t = PAYLOAD_SIZE)]
    pub read_buf_size: usize,
}

impl PushFileArgs {
    pub async fn push_file(&self, async_write: impl AsyncWrite + Unpin) -> io::Result<usize> {
        push_file(&self.source_file, async_write, self.read_buf_size).await
    }
}

pub async fn push_file(
    source_file: impl AsRef<Path>,
    mut async_write: impl AsyncWrite + Unpin,
    read_buf_size: usize,
) -> io::Result<usize> {
    let file = File::open(source_file).await.unwrap();
    let mut file = BufReader::new(file);

    let mut buf = BytesMut::with_capacity(read_buf_size);
    let mut read = 0;
    loop {
        buf.clear();
        let n = file.read_buf(&mut buf).await.unwrap();
        read += n;
        if n == 0 {
            // EOF
            break;
        }

        async_write.write_all(&buf).await.unwrap();
    }
    async_write.flush().await.unwrap();

    Ok(read)
}

#[derive(Debug, Clone, Args)]
pub struct PullFileArgs {
    pub output_file: PathBuf,
}

impl PullFileArgs {
    pub async fn pull_file(&self, async_read: impl AsyncRead + Unpin) -> io::Result<usize> {
        pull_file(&self.output_file, async_read).await
    }
}

pub async fn pull_file(
    output_file: impl AsRef<Path>,
    mut async_read: impl AsyncRead + Unpin,
) -> io::Result<usize> {
    let _ = tokio::fs::remove_file(&output_file).await;
    let mut file = File::options()
        .write(true)
        .create(true)
        .open(output_file)
        .await
        .unwrap();

    let mut buf = BytesMut::new();
    let mut written = 0;
    loop {
        buf.clear();

        match async_read.read_buf(&mut buf).await {
            Ok(n) => written += n,
            Err(_) => break,
        }

        file.write_all(&buf).await.unwrap();
    }

    Ok(written)
}
