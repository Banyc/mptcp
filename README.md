# Multi-path TCP

## How to transfer a file

1. Install Rust: <https://www.rust-lang.org/tools/install>
1. `git clone <REPO_URL>.git`
1. On the server side, run `cargo run -r -p cli --bin server -- <STREAMS> <LISTEN> <COMMAND>`
   - `<STREAMS>`: The maximum number of TCP streams to accept
   - `<LISTEN>`: The listen address
   - `<COMMAND>`: either:
     - `push <SOURCE_FILE>`: To push a file from `<SOURCE_FILE>` to the peer
     - `pull <OUTPUT_FILE>`: To pull a file from the peer to `<OUTPUT_FILE>`
1. On the client side, run `cargo run -r -p cli --bin client -- <STREAMS> <SERVER> <COMMAND>`
   - `<STREAMS>`: The number of TCP streams to connect
   - `<SERVER>`: The server address

## How to use MPTCP in code

Server:

```rust
let listener = MptcpListener::bind(addr, max_session_streams).await.unwrap();
let stream = listener.accept().await.unwrap();
let (mut read, mut write) = stream.into_async_io().into_split();
let mut buf = [0; 13];
read.read_exact(&mut buf).await.unwrap();
write.write_all(b"Hello client!").await.unwrap();
```

Client:

```rust
let stream = MptcpStream::connect(addr, num_streams).await.unwrap();
let (mut read, mut write) = stream.into_async_io().into_split();
write.write_all(b"Hello server!").await.unwrap();
let mut buf = [0; 13];
read.read_exact(&mut buf).await.unwrap();
```
