use std::{net::SocketAddr, path::PathBuf};

use bytes::BytesMut;
use clap::Parser;
use mptcp::{receiver::Receiver, sender::Sender};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Debug, Parser)]
pub struct Args {
    pub streams: usize,
    pub listen: SocketAddr,
    pub output_file: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut write_streams = vec![];
    let mut read_streams = vec![];
    let listener = TcpListener::bind(args.listen).await.unwrap();
    for _ in 0..args.streams {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, write) = stream.into_split();
        write_streams.push(write);
        read_streams.push(read);
    }

    let mut _async_write = Sender::new(write_streams).into_async_write();
    let mut async_read = Receiver::new(read_streams).into_async_read();

    let _ = tokio::fs::remove_file(&args.output_file).await;
    let mut file = File::options()
        .write(true)
        .create(true)
        .open(&args.output_file)
        .await
        .unwrap();

    let mut buf = BytesMut::new();
    let mut written = 0;
    loop {
        buf.clear();

        match async_read.read_buf(&mut buf).await {
            Ok(n) => written += n,
            Err(e) => {
                println!("{e}");
                break;
            }
        }

        file.write_all(&buf).await.unwrap();
    }
    println!("Wrote {written} bytes");
}
