use bytes::{Buf, BytesMut};

#[derive(Debug)]
pub struct DataSegment {
    start_sequence: Sequence,
    payload: BytesMut,
}

impl DataSegment {
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
