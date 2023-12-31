use std::{
    io,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use async_async_io::read::{AsyncAsyncRead, PollRead};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    select,
    sync::{mpsc, Notify},
    task::JoinSet,
};

use crate::{
    message::{DataSegment, Message},
    recv_buf::RecvStreamBuf,
};

const LINGER: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct Receiver {
    recv_buf: Arc<RwLock<RecvStreamBuf>>,
    recv_buf_inserted: Arc<Notify>,
    leftover_data_segment: Option<DataSegment>,
    last_io_error: Arc<Mutex<Option<io::Error>>>,
    recv_tasks: JoinSet<()>,
    _closed: mpsc::Receiver<()>,
}

impl Receiver {
    pub fn new<R>(streams: Vec<R>) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let recv_buf = Arc::new(RwLock::new(RecvStreamBuf::new()));
        let recv_buf_inserted = Arc::new(Notify::new());
        let last_io_error = Arc::new(Mutex::new(None));
        let (closed_tx, closed_rx) = mpsc::channel(1);

        let mut recv_tasks = JoinSet::new();
        for mut stream in streams {
            let recv_buf_inserted = recv_buf_inserted.clone();
            let recv_buf = recv_buf.clone();
            let last_io_error = last_io_error.clone();
            let closed_tx = closed_tx.clone();
            recv_tasks.spawn(async move {
                loop {
                    let res = select! {
                        () = closed_tx.closed() => {
                            // Prevent triggering TCP RST from our side
                            let mut drain_task = JoinSet::new();
                            drain_task.spawn(async move {
                                let mut buf = [0; 1];
                                loop {
                                    let n = stream.read(&mut buf).await?;
                                    if n == 0 {
                                        break;
                                    }
                                }
                                Ok::<_, io::Error>(())
                            });

                            tokio::select! {
                                res = drain_task.join_next() => {
                                    if let Some(task) = res {
                                        // In case the task panicked
                                        let _ = task.unwrap();
                                    }
                                }
                                () = tokio::time::sleep(LINGER) => (),
                            }
                            break;
                        }
                        // `Message::decode` is NOT cancel safe but it's OK if it will not be called again
                        res = Message::decode(&mut stream) => res,
                    };

                    let message = match res {
                        Ok(message) => message,
                        Err(e) => {
                            let mut last_io_error = last_io_error.lock().unwrap();
                            *last_io_error = Some(e);
                            break;
                        }
                    };
                    let data_segment = match message {
                        Message::DataSegment(data_segment) => data_segment,
                        Message::Ping => continue,
                        Message::Shutdown => {
                            let mut last_io_error = last_io_error.lock().unwrap();
                            *last_io_error = None;
                            break;
                        }
                    };

                    {
                        let mut recv_buf = recv_buf.write().unwrap();
                        recv_buf.insert(data_segment);
                    }

                    recv_buf_inserted.notify_waiters();
                }
            });
        }

        Self {
            recv_buf,
            recv_buf_inserted,
            leftover_data_segment: None,
            last_io_error,
            recv_tasks,
            _closed: closed_rx,
        }
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let leftover_data_segment = self.leftover_data_segment.take();

            let mut handle_data_segment = |data_segment: DataSegment| {
                let readable = buf.len().min(data_segment.size());
                buf[..readable].copy_from_slice(&data_segment.payload()[..readable]);
                self.leftover_data_segment = data_segment.advance(readable);
                Ok(readable)
            };

            if let Some(data_segment) = leftover_data_segment {
                return handle_data_segment(data_segment);
            }

            // Checkout receive buffer
            let recv_buf_inserted = self.recv_buf_inserted.notified();
            {
                let mut recv_buf = self.recv_buf.write().unwrap();
                if let Some(data_segment) = recv_buf.pop_first() {
                    drop(recv_buf);
                    return handle_data_segment(data_segment);
                }
            }

            tokio::select! {
                () = recv_buf_inserted => (),
                res = self.recv_tasks.join_next() => {
                    if let Some(task) = res {
                        task.unwrap();
                        continue;
                    }

                    let mut last_io_error = self.last_io_error.lock().unwrap();
                    match last_io_error.take() {
                        Some(e) => return Err(e),
                        None => return Ok(0),
                    }
                }
            }
        }
    }

    pub fn into_async_read(self) -> PollRead<Self> {
        PollRead::new(self)
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        // The drop of `self.closed` will signal receive tasks to end later
        self.recv_tasks.detach_all();
    }
}

impl AsyncAsyncRead for Receiver {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.recv(buf).await {
            Ok(n) => Ok(n),
            Err(_) => Ok(0),
        }
    }
}

#[derive(Debug, Error)]
#[error("No stream left")]
pub struct NoStreamLeft;
