use std::{
    io,
    path::{Path, PathBuf},
};

use clap::{Args, Subcommand};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWrite, BufReader},
};

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
}

impl PushFileArgs {
    pub async fn push_file(&self, async_write: impl AsyncWrite + Unpin) -> io::Result<usize> {
        push_file(&self.source_file, async_write).await
    }
}

pub async fn push_file(
    source_file: impl AsRef<Path>,
    mut async_write: impl AsyncWrite + Unpin,
) -> io::Result<usize> {
    let file = File::open(source_file).await.unwrap();
    let mut file = BufReader::new(file);

    let read = tokio::io::copy(&mut file, &mut async_write).await.unwrap();

    Ok(usize::try_from(read).unwrap())
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

    let written = tokio::io::copy(&mut async_read, &mut file).await.unwrap();

    Ok(usize::try_from(written).unwrap())
}
