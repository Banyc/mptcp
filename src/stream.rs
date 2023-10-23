use std::{io, num::NonZeroUsize, pin::Pin};

use async_async_io::{read::PollRead, write::PollWrite, PollIo};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{tcp, TcpStream, ToSocketAddrs},
    task::JoinSet,
};

use crate::{
    message::{Init, Session},
    receiver::Receiver,
    sender::Sender,
};

#[derive(Debug)]
pub struct MptcpStream {
    poll: PollIo<Receiver, Sender<tcp::OwnedWriteHalf>>,
}

impl MptcpStream {
    pub fn new(
        read_streams: Vec<tcp::OwnedReadHalf>,
        write_streams: Vec<tcp::OwnedWriteHalf>,
    ) -> Self {
        let sender = Sender::new(write_streams);
        let receiver = Receiver::new(read_streams);
        let poll = PollIo::new(PollRead::new(receiver), PollWrite::new(sender));
        Self { poll }
    }

    pub async fn connect(
        addr: impl ToSocketAddrs + Clone + Send + Sync + 'static,
        streams: NonZeroUsize,
    ) -> io::Result<Self> {
        let mut read_streams = vec![];
        let mut write_streams = vec![];
        let session: u64 = rand::random();
        let session = Session::new(session);
        let init = Init::new(session, streams);

        let mut connections = JoinSet::new();
        for _ in 0..streams.get() {
            let addr = addr.clone();
            let init = init.clone();
            connections.spawn(async move {
                let mut stream = TcpStream::connect(&addr).await?;
                init.encode(&mut stream).await?;
                Ok::<_, io::Error>(stream)
            });
        }

        while let Some(task) = connections.join_next().await {
            let stream = task.unwrap()?;
            let (read, write) = stream.into_split();
            read_streams.push(read);
            write_streams.push(write);
        }

        Ok(Self::new(read_streams, write_streams))
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let (read, write) = self.poll.into_split();
        let read = OwnedReadHalf { poll: read };
        let write = OwnedWriteHalf { poll: write };
        (read, write)
    }

    pub fn split(&mut self) -> (ReadHalf, WriteHalf) {
        let (read, write) = self.poll.split_mut();
        let read = ReadHalf { poll: read };
        let write = WriteHalf { poll: write };
        (read, write)
    }
}

impl AsyncRead for MptcpStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.poll).poll_read(cx, buf)
    }
}

impl AsyncWrite for MptcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.poll).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.poll).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.poll).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct OwnedReadHalf {
    poll: PollRead<Receiver>,
}

impl OwnedReadHalf {
    pub fn reunite(self, write: OwnedWriteHalf) -> MptcpStream {
        let poll = PollIo::new(self.poll, write.poll);
        MptcpStream { poll }
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.poll).poll_read(cx, buf)
    }
}

#[derive(Debug)]
pub struct OwnedWriteHalf {
    poll: PollWrite<Sender<tcp::OwnedWriteHalf>>,
}

impl OwnedWriteHalf {
    pub fn reunite(self, read: OwnedReadHalf) -> MptcpStream {
        let poll = PollIo::new(read.poll, self.poll);
        MptcpStream { poll }
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.poll).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.poll).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.poll).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct ReadHalf<'poll> {
    poll: &'poll mut PollRead<Receiver>,
}

impl AsyncRead for ReadHalf<'_> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.poll).poll_read(cx, buf)
    }
}

#[derive(Debug)]
pub struct WriteHalf<'poll> {
    poll: &'poll mut PollWrite<Sender<tcp::OwnedWriteHalf>>,
}

impl AsyncWrite for WriteHalf<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.poll).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.poll).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.poll).poll_shutdown(cx)
    }
}
