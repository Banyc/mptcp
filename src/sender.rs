use std::{collections::VecDeque, io};

use async_async_io::write::{AsyncAsyncWrite, PollWrite};
use bytes::Bytes;
use thiserror::Error;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    task::JoinSet,
};

use crate::{
    message::{Message, Sequence},
    send_buf::SendStreamBuf,
};

/// You will have to explicitly call `Self::shutdown` before the drop
#[derive(Debug)]
pub struct Sender<W> {
    streams: VecDeque<W>,
    next: Sequence,
}

impl<W> Sender<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(streams: Vec<W>) -> Self {
        Self {
            streams: streams.into(),
            next: Sequence::new(0),
        }
    }

    pub async fn batch_send(&mut self, send_buf: &mut SendStreamBuf) -> Result<(), SendError> {
        if self.streams.is_empty() {
            return Err(SendError::NoStreamLeft);
        }

        let mut write_tasks: JoinSet<io::Result<_>> = JoinSet::new();
        let segments = send_buf.iter_unsent_segments();

        for segment in segments {
            let Some(mut stream) = self.streams.pop_front() else {
                break;
            };

            write_tasks.spawn(async move {
                let start_sequence = segment.start_sequence();

                let message = Message::DataSegment(segment);
                message.encode(&mut stream).await?;

                Ok((Some(start_sequence), stream))
            });
        }

        // Send pings for the remaining streams
        while let Some(mut stream) = self.streams.pop_front() {
            write_tasks.spawn(async move {
                Message::Ping.encode(&mut stream).await?;

                Ok((None, stream))
            });
        }

        let mut io_errors = vec![];
        while let Some(task) = write_tasks.join_next().await {
            let res = task.unwrap();
            match res {
                Ok((Some(sequence), stream)) => {
                    self.streams.push_back(stream);
                    send_buf.mark_as_sent(sequence);
                }
                Ok((None, stream)) => {
                    self.streams.push_back(stream);
                }
                Err(e) => {
                    io_errors.push(e);
                }
            }
        }
        if !io_errors.is_empty() {
            return Err(SendError::Io(io_errors));
        }
        Ok(())
    }

    pub async fn batch_send_all(&mut self, data: Bytes) -> Result<(), NoStreamLeft> {
        let data_len = data.len();
        let mut send_buf = SendStreamBuf::new(data, self.next);
        send_buf.split_first_unsent_segment(self.streams.len());

        loop {
            let res = self.batch_send(&mut send_buf).await;
            match res {
                Ok(()) => (),
                Err(SendError::NoStreamLeft) => return Err(NoStreamLeft),
                _ => continue,
            }
            if send_buf.done() {
                self.next = Sequence::new(self.next.inner() + data_len as u64);
                return Ok(());
            }
        }
    }

    pub fn into_async_write(self) -> PollWrite<Self> {
        PollWrite::new(self)
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        let mut last_io_error = None;
        for stream in &mut self.streams {
            if let Err(e) = shutdown_stream(stream).await {
                last_io_error = Some(e);
            }
        }
        match last_io_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<W> AsyncAsyncWrite for Sender<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        {
            // SAFETY: `data` will be dropped outside of this scope
            let data = Bytes::from_static(unsafe { std::mem::transmute(buf) });
            self.batch_send_all(data)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
        }
        Ok(buf.len())
    }

    async fn flush(&mut self) -> io::Result<()> {
        for stream in &mut self.streams {
            stream.flush().await?;
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Self::shutdown(self).await
    }
}

async fn shutdown_stream<W>(stream: &mut W) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let message = Message::Shutdown;
    message.encode(stream).await?;
    stream.shutdown().await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("No stream left")]
    NoStreamLeft,
    #[error("Stream I/O errors")]
    Io(Vec<io::Error>),
}

#[derive(Debug, Error)]
#[error("No stream left")]
pub struct NoStreamLeft;
