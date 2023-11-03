use std::num::NonZeroUsize;

use clap::Parser;
use cli::{print_performance_statistics, FileTransferCommand};
use mptcp::listen::MptcpListener;

#[derive(Debug, Parser)]
pub struct Cli {
    /// The maximum number of TCP streams to accept
    pub streams: NonZeroUsize,
    /// The listen address
    pub listen: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let mut listener = MptcpListener::bind(args.listen, args.streams)
        .await
        .unwrap();
    let stream = listener.accept().await.unwrap();
    let (read, write) = stream.into_split();

    let start = std::time::Instant::now();
    let n = args.file_transfer.perform(read, write).await.unwrap();
    let duration = start.elapsed();
    match &args.file_transfer {
        FileTransferCommand::Push(_) => println!("Read {n} bytes"),
        FileTransferCommand::Pull(_) => println!("Wrote {n} bytes"),
    }
    print_performance_statistics(n, duration);
}
