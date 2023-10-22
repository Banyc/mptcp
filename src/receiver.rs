use std::{
    io,
    sync::{Arc, RwLock},
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

use crate::{message::DataSegment, recv_buf::RecvStreamBuf};

pub struct Receiver {
    recv_buf: Arc<RwLock<RecvStreamBuf>>,
    recv_buf_inserted: Arc<Notify>,
    dead_streams: Arc<Semaphore>,
    num_streams: u32,
    leftover_data_segment: Option<DataSegment>,
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

        let mut recv_tasks = JoinSet::new();
        for mut stream in streams {
            let recv_buf_inserted = recv_buf_inserted.clone();
            let recv_buf = recv_buf.clone();
            let dead_streams = dead_streams.clone();
            recv_tasks.spawn(async move {
                defer! { dead_streams.add_permits(1); };

                loop {
                    let res = DataSegment::decode(&mut stream).await;
                    let data_segment = match res {
                        Ok(Some(data_segment)) => data_segment,
                        Ok(None) => continue,
                        Err(_) => break,
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
            _recv_tasks: recv_tasks,
        }
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, NoStreamLeft> {
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
                    return Err(NoStreamLeft);
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
