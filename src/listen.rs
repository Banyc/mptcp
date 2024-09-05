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
const BACKLOG_MAX: usize = 64;

#[derive(Debug)]
pub struct MptcpListener {
    listeners: Vec<Arc<TcpListener>>,
    complete: tokio::sync::mpsc::Receiver<io::Result<MptcpStream>>,
    _tasks: JoinSet<()>,
}

impl MptcpListener {
    pub async fn bind(
        addrs: impl Iterator<Item = impl ToSocketAddrs>,
        max_session_streams: NonZeroUsize,
    ) -> io::Result<Self> {
        let mut listeners = vec![];
        let (tx, rx) = tokio::sync::mpsc::channel(BACKLOG_MAX);
        let backlog = Arc::new(Backlog::new(
            NonZeroUsize::new(BACKLOG_MAX).unwrap(),
            max_session_streams,
        ));
        let mut tasks = JoinSet::new();
        for addr in addrs {
            let listener = Arc::new(TcpListener::bind(addr).await?);
            for _ in 0..BACKLOG_MAX {
                tasks.spawn({
                    let backlog = Arc::clone(&backlog);
                    let listener = Arc::clone(&listener);
                    let complete = tx.clone();
                    async move {
                        loop {
                            let (stream, _) = match listener.accept().await {
                                Ok(x) => x,
                                Err(e) => {
                                    let _ = complete.send(Err(e)).await;
                                    continue;
                                }
                            };
                            // This call could take `INIT_TIMEOUT` if being attacked
                            backlog.handle(stream, &complete).await;
                        }
                    }
                });
            }
            listeners.push(listener);
        }
        if listeners.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "number of addresses cannot be zero",
            ));
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
            listeners,
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

    pub fn local_addrs(&self) -> impl Iterator<Item = io::Result<SocketAddr>> + '_ {
        self.listeners.iter().map(|listener| listener.local_addr())
    }
}

#[derive(Debug)]
struct Backlog {
    incomplete: RwLock<HashMap<Session, IncompleteConnection>>,
    queue_max: NonZeroUsize,
    max_session_streams: NonZeroUsize,
}

impl Backlog {
    pub fn new(queue_max: NonZeroUsize, max_session_streams: NonZeroUsize) -> Self {
        Self {
            incomplete: RwLock::new(HashMap::new()),
            queue_max,
            max_session_streams,
        }
    }

    pub fn clean(&self, timeout: Duration) {
        self.incomplete
            .write()
            .unwrap()
            .retain(|_, v: &mut IncompleteConnection| !v.timed_out(timeout));
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
                let mut incomplete = this.incomplete.write().unwrap();
                match incomplete.remove(&init.session()) {
                    Some(incomplete_conn) => {
                        let res = incomplete_conn.push(stream);
                        match res {
                            IncompleteConnectionPushResult::Incomplete(incomplete_conn) => {
                                incomplete.insert(init.session(), incomplete_conn);
                                break None;
                            }
                            IncompleteConnectionPushResult::Complete(stream) => break Some(stream),
                        }
                    }
                    None => {
                        if this.queue_max.get() <= incomplete.len() {
                            break None;
                        }
                        let incomplete_conn = IncompleteConnection::new(init.streams());
                        incomplete.insert(init.session(), incomplete_conn);
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
struct IncompleteConnection {
    read_streams: Vec<tcp::OwnedReadHalf>,
    write_streams: Vec<tcp::OwnedWriteHalf>,
    max: NonZeroUsize,
    last_update: Instant,
}

impl IncompleteConnection {
    pub fn new(number: NonZeroUsize) -> Self {
        Self {
            read_streams: Vec::new(),
            write_streams: Vec::new(),
            max: number,
            last_update: Instant::now(),
        }
    }

    pub fn push(mut self, stream: TcpStream) -> IncompleteConnectionPushResult {
        let addr = SingleAddress::Local(stream.local_addr().unwrap());
        let (read, write) = stream.into_split();
        self.read_streams.push(read);
        self.write_streams.push(write);
        if self.read_streams.len() == self.max.get() {
            let stream = MptcpStream::new(self.read_streams, self.write_streams, addr);
            return IncompleteConnectionPushResult::Complete(stream);
        }
        self.last_update = Instant::now();
        IncompleteConnectionPushResult::Incomplete(self)
    }

    pub fn timed_out(&self, timeout: Duration) -> bool {
        self.last_update.elapsed() > timeout
    }
}

#[derive(Debug)]
enum IncompleteConnectionPushResult {
    Incomplete(IncompleteConnection),
    Complete(MptcpStream),
}
