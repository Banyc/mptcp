use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use tokio::{
    net::{tcp, TcpListener, TcpStream, ToSocketAddrs},
    task::JoinSet,
};

use crate::{
    message::{Init, Session},
    stream::MptcpStream,
};

const BACKLOG_TIMEOUT: Duration = Duration::from_secs(60);
const BACKLOG_MAX: usize = 1024;

#[derive(Debug)]
pub struct MptcpListener {
    listener: TcpListener,
    backlog: Arc<RwLock<HashMap<Session, QueuedConnection>>>,
    backlog_max: NonZeroUsize,
    max_session_streams: NonZeroUsize,
    _backlog_cleanup_task: JoinSet<()>,
}

impl MptcpListener {
    pub async fn bind(
        addr: impl ToSocketAddrs,
        max_session_streams: NonZeroUsize,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let backlog = Arc::new(RwLock::new(HashMap::new()));
        let mut backlog_cleanup_task = JoinSet::new();
        backlog_cleanup_task.spawn({
            let backlog = Arc::clone(&backlog);
            async move {
                loop {
                    tokio::time::sleep(BACKLOG_TIMEOUT / 2).await;

                    let mut backlog = backlog.write().unwrap();
                    backlog.retain(|_, v: &mut QueuedConnection| !v.timed_out(BACKLOG_TIMEOUT));
                }
            }
        });

        Ok(Self {
            listener,
            backlog,
            backlog_max: NonZeroUsize::new(BACKLOG_MAX).unwrap(),
            max_session_streams,
            _backlog_cleanup_task: backlog_cleanup_task,
        })
    }

    pub async fn accept(&self) -> io::Result<MptcpStream> {
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            let init = Init::decode(&mut stream).await?;
            if init.streams() > self.max_session_streams {
                continue;
            }
            loop {
                let mut backlog = self.backlog.write().unwrap();
                match backlog.remove(&init.session()) {
                    Some(queued_connection) => {
                        let res = queued_connection.push(stream);
                        match res {
                            QueuedConnectionPushResult::QueuedConnection(queued_connection) => {
                                backlog.insert(init.session(), queued_connection);
                                break;
                            }
                            QueuedConnectionPushResult::Stream(stream) => return Ok(stream),
                        }
                    }
                    None => {
                        if backlog.len() >= self.backlog_max.get() {
                            break;
                        }
                        let queued_connection = QueuedConnection::new(init.streams());
                        backlog.insert(init.session(), queued_connection);
                    }
                };
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

#[derive(Debug)]
struct QueuedConnection {
    read_streams: Vec<tcp::OwnedReadHalf>,
    write_streams: Vec<tcp::OwnedWriteHalf>,
    max: NonZeroUsize,
    last_update: Instant,
}

impl QueuedConnection {
    pub fn new(number: NonZeroUsize) -> Self {
        Self {
            read_streams: Vec::new(),
            write_streams: Vec::new(),
            max: number,
            last_update: Instant::now(),
        }
    }

    pub fn push(mut self, stream: TcpStream) -> QueuedConnectionPushResult {
        let (read, write) = stream.into_split();
        self.read_streams.push(read);
        self.write_streams.push(write);
        if self.read_streams.len() == self.max.get() {
            let stream = MptcpStream::new(self.read_streams, self.write_streams);
            return QueuedConnectionPushResult::Stream(stream);
        }
        self.last_update = Instant::now();
        QueuedConnectionPushResult::QueuedConnection(self)
    }

    pub fn timed_out(&self, timeout: Duration) -> bool {
        self.last_update.elapsed() > timeout
    }
}

#[derive(Debug)]
enum QueuedConnectionPushResult {
    QueuedConnection(QueuedConnection),
    Stream(MptcpStream),
}
