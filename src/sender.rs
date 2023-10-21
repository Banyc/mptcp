use std::{collections::VecDeque, io};

use bytes::Bytes;
use thiserror::Error;
use tokio::{net::TcpStream, task::JoinSet};

use crate::send_buf::SendStreamBuf;

pub struct Sender {
    streams: VecDeque<TcpStream>,
    send_buf: SendStreamBuf,
}

impl Sender {
    pub fn new(streams: Vec<TcpStream>, data: Bytes) -> Self {
        let mut send_buf = SendStreamBuf::new(data);
        send_buf.split_first_unsent_segment(streams.len());
        Self {
            streams: streams.into(),
            send_buf,
        }
    }

    pub async fn batch_send(&mut self) -> Result<(), SendError> {
        if self.streams.is_empty() {
            return Err(SendError::NoStreamLeft);
        }

        let mut write_tasks: JoinSet<io::Result<_>> = JoinSet::new();
        let segments = self.send_buf.iter_unsent_segments();

        for segment in segments {
            let mut stream = match self.streams.pop_front() {
                Some(stream) => stream,
                None => break,
            };

            write_tasks.spawn(async move {
                segment.encode(&mut stream).await?;

                Ok((segment.start_sequence(), stream))
            });
        }

        let mut io_errors = vec![];
        while let Some(task) = write_tasks.join_next().await {
            let res = task.unwrap();
            match res {
                Ok((sequence, stream)) => {
                    self.streams.push_back(stream);
                    self.send_buf.mark_as_sent(sequence);
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

    pub async fn batch_send_all(&mut self) -> Result<(), NoStreamLeft> {
        loop {
            let res = self.batch_send().await;
            match res {
                Ok(()) => (),
                Err(SendError::NoStreamLeft) => return Err(NoStreamLeft),
                _ => continue,
            }
            if self.send_buf.done() {
                return Ok(());
            }
        }
    }
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
