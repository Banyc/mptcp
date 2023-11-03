use clap::Parser;
use cli::{print_performance_statistics, FileTransferCommand, Protocol};
use mptcp::stream::MptcpStream;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The server address
    pub server: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (protocol, internet_address) = args.server.split_once("://").unwrap();
    let protocol: Protocol = protocol.parse().expect("Unknown protocol");
    let (read, write): (Box<dyn AsyncRead + Unpin>, Box<dyn AsyncWrite + Unpin>) = match protocol {
        Protocol::Tcp => {
            let stream = TcpStream::connect(internet_address).await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        Protocol::Mptcp { streams } => {
            let stream = MptcpStream::connect(internet_address.to_string(), streams)
                .await
                .unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
    };

    let start = std::time::Instant::now();
    let n = args.file_transfer.perform(read, write).await.unwrap();
    let duration = start.elapsed();
    match &args.file_transfer {
        FileTransferCommand::Push(_) => println!("Read {n} bytes"),
        FileTransferCommand::Pull(_) => println!("Wrote {n} bytes"),
    }
    print_performance_statistics(n, duration);
}
