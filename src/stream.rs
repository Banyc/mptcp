use std::{io, net::SocketAddr, num::NonZeroUsize, pin::Pin};

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
    addr: SingleAddress,
}

impl MptcpStream {
    pub(crate) fn new(
        read_streams: Vec<tcp::OwnedReadHalf>,
        write_streams: Vec<tcp::OwnedWriteHalf>,
        addr: SingleAddress,
    ) -> Self {
        let sender = Sender::new(write_streams);
        let receiver = Receiver::new(read_streams);
        let poll = PollIo::new(PollRead::new(receiver), PollWrite::new(sender));
        Self { poll, addr }
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
                let peer_addr = stream.peer_addr().unwrap();
                init.encode(&mut stream).await?;
                Ok::<_, io::Error>((stream, peer_addr))
            });
        }

        let mut last_peer_addr = None;
        while let Some(task) = connections.join_next().await {
            let (stream, peer_addr) = task.unwrap()?;
            last_peer_addr = Some(peer_addr);
            let (read, write) = stream.into_split();
            read_streams.push(read);
            write_streams.push(write);
        }

        let addr = SingleAddress::Peer(last_peer_addr.unwrap());

        Ok(Self::new(read_streams, write_streams, addr))
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let (read, write) = self.poll.into_split();
        let addr = self.addr;
        let read = OwnedReadHalf { poll: read, addr };
        let write = OwnedWriteHalf { poll: write, addr };
        (read, write)
    }

    pub fn split(&mut self) -> (ReadHalf, WriteHalf) {
        let (read, write) = self.poll.split_mut();
        let addr = self.addr;
        let read = ReadHalf { poll: read, addr };
        let write = WriteHalf { poll: write, addr };
        (read, write)
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Local(addr) = self.addr {
            return Some(addr);
        }
        None
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Peer(addr) = self.addr {
            return Some(addr);
        }
        None
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
    addr: SingleAddress,
}

impl OwnedReadHalf {
    pub fn reunite(self, write: OwnedWriteHalf) -> MptcpStream {
        let poll = PollIo::new(self.poll, write.poll);
        let addr = self.addr;
        MptcpStream { poll, addr }
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Local(addr) = self.addr {
            return Some(addr);
        }
        None
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Peer(addr) = self.addr {
            return Some(addr);
        }
        None
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
    addr: SingleAddress,
}

impl OwnedWriteHalf {
    pub fn reunite(self, read: OwnedReadHalf) -> MptcpStream {
        let poll = PollIo::new(read.poll, self.poll);
        let addr = self.addr;
        MptcpStream { poll, addr }
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Local(addr) = self.addr {
            return Some(addr);
        }
        None
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Peer(addr) = self.addr {
            return Some(addr);
        }
        None
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
    addr: SingleAddress,
}

impl ReadHalf<'_> {
    pub fn local_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Local(addr) = self.addr {
            return Some(addr);
        }
        None
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Peer(addr) = self.addr {
            return Some(addr);
        }
        None
    }
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
    addr: SingleAddress,
}

impl WriteHalf<'_> {
    pub fn local_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Local(addr) = self.addr {
            return Some(addr);
        }
        None
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        if let SingleAddress::Peer(addr) = self.addr {
            return Some(addr);
        }
        None
    }
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

#[derive(Debug, Clone, Copy)]
pub(crate) enum SingleAddress {
    Local(SocketAddr),
    Peer(SocketAddr),
}
