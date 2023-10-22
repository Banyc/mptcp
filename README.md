# Multi-path TCP

## How to perform file transfers

1. Install Rust: <https://www.rust-lang.org/tools/install>
1. `git clone <REPO_URL>.git`
1. On the server side, run `cargo run -r -p cli --bin server -- <LISTEN> <COMMAND>`
   - `<LISTEN>`: The listen address
   - `<COMMAND>`: either:
     - `push <SOURCE_FILE>`: To push a file from `<SOURCE_FILE>` to the peer
     - `pull <OUTPUT_FILE>`: To pull a file from the peer to `<OUTPUT_FILE>`
1. On the client side, run `cargo run -r -p cli --bin client -- <STREAMS> <SERVER> <COMMAND>`
   - `<STREAMS>`: The number of streams to connect
   - `<SERVER>`: The server address
