use std::collections::BTreeMap;

use bytes::Bytes;

use crate::message::Sequence;

pub struct SendStreamBuf {
    data: Bytes,
    unsent_segments: BTreeMap<Sequence, usize>,
}

impl SendStreamBuf {
    pub fn new(data: Bytes) -> Self {
        let mut unsent = BTreeMap::new();
        if !data.is_empty() {
            unsent.insert(Sequence::new(0), data.len());
        }
        Self {
            data,
            unsent_segments: unsent,
        }
    }

    pub fn done(&self) -> bool {
        self.unsent_segments.is_empty()
    }

    /// Best-effect
    pub fn split_first_unsent_segment(&mut self, segments: usize) {
        if self.unsent_segments.len() >= segments {
            return;
        }
        let (sequence, length) = match self.unsent_segments.pop_first() {
            Some(segment) => segment,
            None => return,
        };

        let segment_bytes = length.div_ceil(segments);
        if segment_bytes == 0 {
            return;
        }

        // e.g.,
        // `|      17       |`
        // `|  6  |  6  | 5 |`
        let mut next_sequence = sequence;
        let mut remaining_bytes = length;
        while remaining_bytes > 0 {
            let length = segment_bytes.min(remaining_bytes);
            self.unsent_segments.insert(next_sequence, length);
            remaining_bytes -= length;
            next_sequence = Sequence::new(next_sequence.inner() + length as u64);
        }
    }

    pub fn iter_unsent_segments(&self) -> impl Iterator<Item = DataSegment> + '_ {
        self.unsent_segments.iter().map(|(start_sequence, length)| {
            let range =
                start_sequence.inner() as usize..(start_sequence.inner() as usize + *length);
            let payload = self.data.slice(range);
            DataSegment::new(*start_sequence, payload).unwrap()
        })
    }

    pub fn mark_as_sent(&mut self, sequence: Sequence) {
        self.unsent_segments.remove(&sequence);
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

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}
