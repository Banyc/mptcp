use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
}

#[derive(Debug)]
pub struct DataSegmentMut {
    start_sequence: Sequence,
    payload: BytesMut,
}

impl DataSegmentMut {
    pub fn new(start_sequence: Sequence, payload: BytesMut) -> Option<Self> {
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

    pub async fn decode<R>(reader: &mut R) -> io::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let start_sequence = reader.read_u64().await?;
        let length = reader.read_u64().await?;
        let mut payload = BytesMut::with_capacity(length as usize);
        reader.read_buf(&mut payload).await?;
        Ok(Self {
            start_sequence: Sequence::new(start_sequence),
            payload,
        })
    }

    pub fn payload(&self) -> &BytesMut {
        &self.payload
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
        let dst = DataSegmentMut::decode(&mut reader).await.unwrap();
        assert_eq!(src.start_sequence(), dst.start_sequence());
        assert_eq!(src.payload(), dst.payload());
    }
}
