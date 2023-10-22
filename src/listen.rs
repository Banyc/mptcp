use std::{collections::HashMap, io, num::NonZeroUsize};

use tokio::net::{tcp, TcpListener, TcpStream, ToSocketAddrs};

use crate::{
    message::{Init, Session},
    stream::MptcpStream,
};

pub struct MptcpListener {
    listener: TcpListener,
    backlog: HashMap<Session, Backlog>,
}

impl MptcpListener {
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            backlog: HashMap::new(),
        })
    }

    pub async fn accept(&mut self) -> io::Result<MptcpStream> {
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            let init = Init::decode(&mut stream).await?;
            loop {
                match self.backlog.remove(&init.session()) {
                    Some(backlog) => {
                        let res = backlog.push(stream);
                        match res {
                            BacklogPushResult::Backlog(backlog) => {
                                self.backlog.insert(init.session(), backlog);
                                break;
                            }
                            BacklogPushResult::Stream(stream) => return Ok(stream),
                        }
                    }
                    None => {
                        let backlog = Backlog::new(init.streams());
                        self.backlog.insert(init.session(), backlog);
                    }
                };
            }
        }
    }
}

struct Backlog {
    read_streams: Vec<tcp::OwnedReadHalf>,
    write_streams: Vec<tcp::OwnedWriteHalf>,
    max: NonZeroUsize,
}

impl Backlog {
    pub fn new(number: NonZeroUsize) -> Self {
        Self {
            read_streams: Vec::new(),
            write_streams: Vec::new(),
            max: number,
        }
    }

    pub fn push(mut self, stream: TcpStream) -> BacklogPushResult {
        let (read, write) = stream.into_split();
        self.read_streams.push(read);
        self.write_streams.push(write);
        if self.read_streams.len() == self.max.get() {
            let stream = MptcpStream::new(self.read_streams, self.write_streams);
            return BacklogPushResult::Stream(stream);
        }
        BacklogPushResult::Backlog(self)
    }
}

enum BacklogPushResult {
    Backlog(Backlog),
    Stream(MptcpStream),
}
