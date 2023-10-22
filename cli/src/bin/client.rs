use std::path::PathBuf;

use bytes::BytesMut;
use clap::Parser;
use mptcp::{receiver::Receiver, sender::Sender};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

const PAYLOAD_SIZE: usize = 1024;

#[derive(Debug, Parser)]
pub struct Args {
    pub streams: usize,
    pub server: String,
    pub file: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut write_streams = vec![];
    let mut read_streams = vec![];
    for _ in 0..args.streams {
        let stream = TcpStream::connect(&args.server).await.unwrap();
        let (read, write) = stream.into_split();
        write_streams.push(write);
        read_streams.push(read);
    }

    let mut async_write = Sender::new(write_streams).into_async_write();
    let mut _async_read = Receiver::new(read_streams).into_async_read();

    let file = File::open(&args.file).await.unwrap();
    let mut file = BufReader::new(file);

    let mut buf = BytesMut::with_capacity(args.streams * PAYLOAD_SIZE);
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
    println!("Read {read} bytes");
}
