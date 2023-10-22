use std::{
    io,
    sync::{Arc, Mutex, RwLock},
};

use async_async_io::read::{AsyncAsyncRead, PollRead};
use async_trait::async_trait;
use scopeguard::defer;
use thiserror::Error;
use tokio::{
    io::AsyncRead,
    sync::{Notify, Semaphore},
    task::JoinSet,
};

use crate::{
    message::{DataSegment, Message},
    recv_buf::RecvStreamBuf,
};

pub struct Receiver {
    recv_buf: Arc<RwLock<RecvStreamBuf>>,
    recv_buf_inserted: Arc<Notify>,
    dead_streams: Arc<Semaphore>,
    num_streams: u32,
    leftover_data_segment: Option<DataSegment>,
    last_io_error: Arc<Mutex<Option<io::Error>>>,
    _recv_tasks: JoinSet<()>,
}

impl Receiver {
    pub fn new<R>(streams: Vec<R>) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let recv_buf = Arc::new(RwLock::new(RecvStreamBuf::new()));
        let recv_buf_inserted = Arc::new(Notify::new());
        let dead_streams = Arc::new(Semaphore::new(0));
        let num_streams = u32::try_from(streams.len()).unwrap();
        let last_io_error = Arc::new(Mutex::new(None));

        let mut recv_tasks = JoinSet::new();
        for mut stream in streams {
            let recv_buf_inserted = recv_buf_inserted.clone();
            let recv_buf = recv_buf.clone();
            let dead_streams = dead_streams.clone();
            let last_io_error = last_io_error.clone();
            recv_tasks.spawn(async move {
                defer! { dead_streams.add_permits(1); };

                loop {
                    let res = Message::decode(&mut stream).await;
                    let message = match res {
                        Ok(Some(message)) => message,
                        Ok(None) => continue,
                        Err(e) => {
                            let mut last_io_error = last_io_error.lock().unwrap();
                            *last_io_error = Some(e);
                            break;
                        }
                    };
                    let data_segment = match message {
                        Message::DataSegment(data_segment) => data_segment,
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
            dead_streams,
            num_streams,
            leftover_data_segment: None,
            last_io_error,
            _recv_tasks: recv_tasks,
        }
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let recv_buf_inserted = self.recv_buf_inserted.notified();
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

            {
                let mut recv_buf = self.recv_buf.write().unwrap();
                if let Some(data_segment) = recv_buf.pop_first() {
                    return handle_data_segment(data_segment);
                }
            }

            tokio::select! {
                () = recv_buf_inserted => (),
                res = self.dead_streams.acquire_many(self.num_streams) => {
                    let _ = res.unwrap();
                    self.dead_streams.add_permits(self.num_streams as usize);
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

#[async_trait]
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
