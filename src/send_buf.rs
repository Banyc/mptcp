use std::collections::BTreeMap;

use bytes::Bytes;

use crate::message::{DataSegment, Sequence};

const MINIMUM_PAYLOAD_SIZE: usize = 1024;

#[derive(Debug)]
pub struct SendStreamBuf {
    data: Bytes,
    unsent_segments: BTreeMap<Sequence, usize>,
    start_sequence: Sequence,
}

impl SendStreamBuf {
    pub fn new(data: Bytes, start_sequence: Sequence) -> Self {
        let mut unsent = BTreeMap::new();
        if !data.is_empty() {
            unsent.insert(start_sequence, data.len());
        }
        Self {
            data,
            unsent_segments: unsent,
            start_sequence,
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
        let Some((sequence, length)) = self.unsent_segments.pop_first() else {
            return;
        };

        let segment_bytes = length.div_ceil(segments).max(MINIMUM_PAYLOAD_SIZE);
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
            let start = start_sequence.inner() - self.start_sequence.inner();
            let start = usize::try_from(start).unwrap();
            let range = start..(start + *length);
            let payload = self.data.slice(range);
            DataSegment::new(*start_sequence, payload).unwrap()
        })
    }

    pub fn mark_as_sent(&mut self, sequence: Sequence) {
        self.unsent_segments.remove(&sequence);
    }
}
