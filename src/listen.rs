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
    stream::{MptcpStream, SingleAddress},
};

const BACKLOG_TIMEOUT: Duration = Duration::from_secs(60);
const INIT_TIMEOUT: Duration = Duration::from_secs(1);
const BACKLOG_MAX: usize = 1024;

#[derive(Debug)]
pub struct MptcpListener {
    listener: Arc<TcpListener>,
    complete: tokio::sync::mpsc::Receiver<io::Result<MptcpStream>>,
    _tasks: JoinSet<()>,
}

impl MptcpListener {
    pub async fn bind(
        addr: impl ToSocketAddrs,
        max_session_streams: NonZeroUsize,
    ) -> io::Result<Self> {
        let listener = Arc::new(TcpListener::bind(addr).await?);
        let (tx, rx) = tokio::sync::mpsc::channel(BACKLOG_MAX);

        let backlog = Arc::new(Backlog::new(
            NonZeroUsize::new(BACKLOG_MAX).unwrap(),
            max_session_streams,
        ));

        let mut tasks = JoinSet::new();
        for _ in 0..BACKLOG_MAX {
            let backlog = Arc::clone(&backlog);
            let listener = Arc::clone(&listener);
            let complete = tx.clone();
            tasks.spawn(async move {
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(x) => x,
                        Err(e) => {
                            let _ = complete.send(Err(e)).await;
                            continue;
                        }
                    };
                    backlog.handle(stream, &complete).await;
                }
            });
        }
        tasks.spawn({
            let backlog = Arc::clone(&backlog);
            async move {
                loop {
                    tokio::time::sleep(BACKLOG_TIMEOUT / 2).await;

                    backlog.clean(BACKLOG_TIMEOUT);
                }
            }
        });

        Ok(Self {
            listener,
            complete: rx,
            _tasks: tasks,
        })
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `accept` is used as the event in a `tokio::select` statement and some other branch completes first, it is guaranteed that no streams were received on this listener.
    pub async fn accept(&mut self) -> io::Result<MptcpStream> {
        self.complete
            .recv()
            .await
            .expect("senders will never drop proactively")
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

#[derive(Debug)]
struct Backlog {
    queued: RwLock<HashMap<Session, QueuedConnection>>,
    queue_max: NonZeroUsize,
    max_session_streams: NonZeroUsize,
}

impl Backlog {
    pub fn new(queue_max: NonZeroUsize, max_session_streams: NonZeroUsize) -> Self {
        Self {
            queued: RwLock::new(HashMap::new()),
            queue_max,
            max_session_streams,
        }
    }

    pub fn clean(&self, timeout: Duration) {
        self.queued
            .write()
            .unwrap()
            .retain(|_, v: &mut QueuedConnection| !v.timed_out(timeout));
    }

    pub async fn handle(
        &self,
        stream: TcpStream,
        complete: &tokio::sync::mpsc::Sender<io::Result<MptcpStream>>,
    ) {
        async fn handle_impl(
            this: &Backlog,
            mut stream: TcpStream,
            complete: &tokio::sync::mpsc::Sender<io::Result<MptcpStream>>,
        ) -> io::Result<()> {
            let init = tokio::time::timeout(INIT_TIMEOUT, Init::decode(&mut stream)).await??;
            if init.streams() > this.max_session_streams {
                return Ok(());
            }

            let stream = loop {
                let mut queued = this.queued.write().unwrap();
                match queued.remove(&init.session()) {
                    Some(queued_connection) => {
                        let res = queued_connection.push(stream);
                        match res {
                            QueuedConnectionPushResult::QueuedConnection(queued_connection) => {
                                queued.insert(init.session(), queued_connection);
                                break None;
                            }
                            QueuedConnectionPushResult::Stream(stream) => break Some(stream),
                        }
                    }
                    None => {
                        if queued.len() >= this.queue_max.get() {
                            break None;
                        }
                        let queued_connection = QueuedConnection::new(init.streams());
                        queued.insert(init.session(), queued_connection);
                    }
                }
            };
            if let Some(stream) = stream {
                let _ = complete.send(Ok(stream)).await;
            }
            Ok(())
        }
        let _ = handle_impl(self, stream, complete).await;
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
        let addr = SingleAddress::Local(stream.local_addr().unwrap());
        let (read, write) = stream.into_split();
        self.read_streams.push(read);
        self.write_streams.push(write);
        if self.read_streams.len() == self.max.get() {
            let stream = MptcpStream::new(self.read_streams, self.write_streams, addr);
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
