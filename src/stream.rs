use std::{io, num::NonZeroUsize};

use async_async_io::PollIo;
use tokio::{
    net::{tcp, TcpStream, ToSocketAddrs},
    task::JoinSet,
};

use crate::{
    message::{Init, Session},
    receiver::Receiver,
    sender::Sender,
};

pub struct MptcpStream {
    sender: Sender<tcp::OwnedWriteHalf>,
    receiver: Receiver,
}

impl MptcpStream {
    pub fn new(
        read_streams: Vec<tcp::OwnedReadHalf>,
        write_streams: Vec<tcp::OwnedWriteHalf>,
    ) -> Self {
        let sender = Sender::new(write_streams);
        let receiver = Receiver::new(read_streams);
        Self { sender, receiver }
    }

    pub fn into_async_io(self) -> PollIo<Receiver, Sender<tcp::OwnedWriteHalf>> {
        PollIo::new(
            self.receiver.into_async_read(),
            self.sender.into_async_write(),
        )
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
}
