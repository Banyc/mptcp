use std::{
    io,
    sync::{Arc, RwLock},
};

use async_async_io::read::AsyncAsyncRead;
use async_trait::async_trait;
use scopeguard::defer;
use thiserror::Error;
use tokio::{
    io::AsyncRead,
    sync::{Notify, Semaphore},
    task::JoinSet,
};

use crate::{message::DataSegmentMut, recv_buf::RecvStreamBuf};

pub struct Receiver {
    recv_buf: Arc<RwLock<RecvStreamBuf>>,
    data_segment: Option<DataSegmentMut>,
    _recv_tasks: JoinSet<()>,
    recv_buf_inserted: Arc<Notify>,
    no_stream_left: Arc<Semaphore>,
    num_streams: u32,
}

impl Receiver {
    pub fn new<R>(streams: Vec<R>) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let recv_buf = Arc::new(RwLock::new(RecvStreamBuf::new()));
        let recv_buf_inserted = Arc::new(Notify::new());
        let no_stream_left = Arc::new(Semaphore::new(0));
        let num_streams = u32::try_from(streams.len()).unwrap();

        let mut recv_tasks = JoinSet::new();
        for mut stream in streams {
            let recv_buf_inserted = recv_buf_inserted.clone();
            let recv_buf = recv_buf.clone();
            let no_stream_left = no_stream_left.clone();
            recv_tasks.spawn(async move {
                defer! { no_stream_left.add_permits(1); };

                loop {
                    let res = DataSegmentMut::decode(&mut stream).await;
                    let data_segment = match res {
                        Ok(data_segment) => data_segment,
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
            _recv_tasks: recv_tasks,
            recv_buf_inserted,
            data_segment: None,
            no_stream_left,
            num_streams,
        }
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, NoStreamLeft> {
        loop {
            let recv_buf_inserted = self.recv_buf_inserted.notified();
            let data_segment = self.data_segment.take();

            let mut handle_data_segment = |data_segment: DataSegmentMut| {
                let readable = buf.len().min(data_segment.size());
                buf[..readable].copy_from_slice(&data_segment.payload()[..readable]);
                let data_segment = data_segment.advance(readable);
                self.data_segment = data_segment;
                Ok(readable)
            };

            if let Some(data_segment) = data_segment {
                return handle_data_segment(data_segment);
            }

            {
                let mut recv_buf = self.recv_buf.write().unwrap();
                if let Some(data_segment) = recv_buf.pop_first() {
                    return handle_data_segment(data_segment);
                }
            }

            tokio::select! {
                res = self.no_stream_left.acquire_many(self.num_streams) => {
                    let _ = res.unwrap();
                    return Err(NoStreamLeft);
                }
                () = recv_buf_inserted => (),
            }
        }
    }
}

#[async_trait]
impl AsyncAsyncRead for Receiver {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv(buf)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, e))
    }
}

#[derive(Debug, Error)]
#[error("No stream left")]
pub struct NoStreamLeft;
