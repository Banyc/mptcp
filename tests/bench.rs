use std::{net::SocketAddr, num::NonZeroUsize, process::exit, time::Instant};

use bytes::BytesMut;
use mptcp::{MptcpListener, MptcpStream};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};

const STREAMS: usize = 4;
const PAYLOAD_SIZE: usize = 1 << 24;

async fn bench_server() -> (SocketAddr, JoinSet<()>) {
    let mut server = MptcpListener::bind("127.0.0.1:0", NonZeroUsize::new(STREAMS).unwrap())
        .await
        .unwrap();
    let listen_addr = server.local_addr().unwrap();
    let mut listen_task = JoinSet::new();
    listen_task.spawn(async move {
        let mut buf = BytesMut::new();
        let mut stream = server.accept().await.unwrap();
        loop {
            stream.read_buf(&mut buf).await.unwrap();
        }
    });
    (listen_addr, listen_task)
}

async fn bench_client(listen_addr: SocketAddr) {
    const ROUNDS: usize = 100;
    let mut client = MptcpStream::connect(listen_addr, NonZeroUsize::new(STREAMS).unwrap())
        .await
        .unwrap();
    let buf: Vec<u8> = (0..PAYLOAD_SIZE).map(|_| rand::random()).collect();
    let start = Instant::now();
    for _ in 0..ROUNDS {
        client.write_all(&buf).await.unwrap();
    }
    let duration = start.elapsed();
    let throughput = (buf.len() * ROUNDS) as f64 / duration.as_secs_f64();
    let throughput_mib_s = throughput / 1024. / 1024.;
    let latency_ms = duration.as_secs_f64() * 1000. / ROUNDS as f64;
    println!("throughput: {throughput_mib_s:.2} MiB/s, latency: {latency_ms:.2} ms");
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench() {
    let (addr, _task) = bench_server().await;
    bench_client(addr).await;
    exit(0);
}
