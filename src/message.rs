use std::{io, num::NonZeroUsize};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const DATA_SEGMENT_TYPE_CODE: u8 = 0;
const PING_TYPE_CODE: u8 = 1;
const SHUTDOWN_TYPE_CODE: u8 = 2;

#[derive(Debug)]
pub enum Message {
    DataSegment(DataSegment),
    Ping,
    Shutdown,
}

impl Message {
    pub async fn encode<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match self {
            Message::DataSegment(data_segment) => {
                writer.write_u8(DATA_SEGMENT_TYPE_CODE).await?;
                data_segment.encode(writer).await?;
            }
            Message::Ping => writer.write_u8(PING_TYPE_CODE).await?,
            Message::Shutdown => writer.write_u8(SHUTDOWN_TYPE_CODE).await?,
        }
        Ok(())
    }

    pub async fn decode<R>(reader: &mut R) -> io::Result<Option<Self>>
    where
        R: AsyncRead + Unpin,
    {
        let type_code = reader.read_u8().await?;
        let this = match type_code {
            DATA_SEGMENT_TYPE_CODE => {
                let data_segment = match DataSegment::decode(reader).await? {
                    Some(data_segment) => data_segment,
                    None => return Ok(None),
                };
                Self::DataSegment(data_segment)
            }
            PING_TYPE_CODE => Self::Ping,
            SHUTDOWN_TYPE_CODE => Self::Shutdown,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown type code: {type_code}"),
                ))
            }
        };
        Ok(Some(this))
    }
}

#[derive(Debug)]
pub struct DataSegment {
    start_sequence: Sequence,
    payload: Bytes,
}

impl DataSegment {
    pub fn new(start_sequence: Sequence, payload: Bytes) -> Option<Self> {
        if payload.is_empty() {
            return None;
        }

        Some(Self {
            start_sequence,
            payload,
        })
    }

    pub fn start_sequence(&self) -> Sequence {
        self.start_sequence
    }

    pub fn end_sequence(&self) -> Sequence {
        Sequence::new(self.start_sequence.inner() + self.payload.len() as u64)
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub async fn encode<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_u64(self.start_sequence.inner()).await?;
        writer.write_u64(self.payload.len() as u64).await?;
        writer.write_all(&self.payload).await?;
        Ok(())
    }

    pub fn advance(mut self, bytes: usize) -> Option<Self> {
        if self.payload.len() <= bytes {
            return None;
        }

        self.start_sequence = Sequence::new(self.start_sequence.inner() + bytes as u64);
        self.payload.advance(bytes);

        Some(self)
    }

    pub fn advance_to(self, end: Sequence) -> Option<Self> {
        let bytes = end.inner().saturating_sub(self.start_sequence.inner());
        let bytes = match usize::try_from(bytes) {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };

        self.advance(bytes)
    }

    pub fn size(&self) -> usize {
        self.payload.len()
    }

    pub async fn decode<R>(reader: &mut R) -> io::Result<Option<Self>>
    where
        R: AsyncRead + Unpin,
    {
        let start_sequence = reader.read_u64().await?;
        let length = reader.read_u64().await?;
        let length =
            usize::try_from(length).map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))?;
        let mut payload = BytesMut::with_capacity(length);
        payload.put_bytes(0, length);
        reader.read_exact(&mut payload[..]).await?;
        Ok(Self::new(Sequence::new(start_sequence), payload.into()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash)]
pub struct Sequence(u64);

impl Sequence {
    pub fn new(number: u64) -> Self {
        Self(number)
    }

    pub fn inner(&self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub struct Init {
    session: Session,
    streams: NonZeroUsize,
}

impl Init {
    pub fn new(session: Session, streams: NonZeroUsize) -> Self {
        Self { session, streams }
    }

    pub fn session(&self) -> Session {
        self.session
    }

    pub fn streams(&self) -> NonZeroUsize {
        self.streams
    }

    pub async fn encode<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_u64(self.session.inner()).await?;
        writer.write_u64(self.streams.get() as u64).await?;
        Ok(())
    }

    pub async fn decode<R>(reader: &mut R) -> io::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let session = reader.read_u64().await?;
        let session = Session::new(session);
        let streams = reader.read_u64().await?;
        let streams =
            usize::try_from(streams).map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e))?;
        let streams = NonZeroUsize::new(streams)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "zero streams"))?;
        Ok(Self { session, streams })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, std::hash::Hash)]
pub struct Session(u64);

impl Session {
    pub fn new(number: u64) -> Self {
        Self(number)
    }

    pub fn inner(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_data_segment_codec() {
        let src =
            DataSegment::new(Sequence(42), Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])).unwrap();
        let mut buf = vec![];
        src.encode(&mut buf).await.unwrap();
        let mut reader = io::Cursor::new(&buf[..]);
        let dst = DataSegment::decode(&mut reader).await.unwrap().unwrap();
        assert_eq!(src.start_sequence(), dst.start_sequence());
        assert_eq!(src.payload(), dst.payload());
    }
}
