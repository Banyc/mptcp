use std::collections::BTreeMap;

use crate::message::{DataSegment, Sequence};

#[derive(Debug)]
pub struct RecvStreamBuf {
    next: Sequence,
    data_segments: BTreeMap<Sequence, DataSegment>,
}

impl RecvStreamBuf {
    pub fn new() -> Self {
        Self {
            next: Sequence::new(0),
            data_segments: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, data_segment: DataSegment) {
        // Remove stale data
        let Some(data_segment) = data_segment.advance_to(self.next) else {
            return;
        };

        // Resolve key conflict
        if let Some(old_data_segment) = self.data_segments.get(&data_segment.start_sequence()) {
            if data_segment.size() <= old_data_segment.size() {
                return;
            }
        }

        self.data_segments
            .insert(data_segment.start_sequence(), data_segment);
    }

    pub fn pop_first(&mut self) -> Option<DataSegment> {
        let (first_key, _) = self.data_segments.first_key_value()?;
        if *first_key != self.next {
            return None;
        }

        let (_, first_segment) = self.data_segments.pop_first().unwrap();

        self.next = first_segment.end_sequence();

        // Deduplicate data
        loop {
            if let Some((start_sequence, _)) = self.data_segments.first_key_value() {
                if self.next <= *start_sequence {
                    break;
                }
            }
            let Some((_, data_segment)) = self.data_segments.pop_first() else {
                break;
            };

            let data_segment = data_segment.advance_to(self.next);
            if let Some(data_segment) = data_segment {
                self.data_segments
                    .insert(data_segment.start_sequence(), data_segment);
            }
        }

        Some(first_segment)
    }
}

impl Default for RecvStreamBuf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn basic() {
        let mut buf = RecvStreamBuf::new();
        buf.insert(DataSegment::new(Sequence::new(0), Bytes::from_iter(vec![0, 1, 2])).unwrap());
        let data_segment = buf.pop_first().unwrap();
        assert_eq!(buf.next, data_segment.end_sequence());
        assert!(buf.pop_first().is_none());
    }

    #[test]
    fn unordered() {
        let mut buf = RecvStreamBuf::new();
        buf.insert(DataSegment::new(Sequence::new(1), Bytes::from_iter(vec![1])).unwrap());
        assert!(buf.pop_first().is_none());
        buf.insert(DataSegment::new(Sequence::new(2), Bytes::from_iter(vec![2])).unwrap());
        assert!(buf.pop_first().is_none());
        buf.insert(DataSegment::new(Sequence::new(0), Bytes::from_iter(vec![0])).unwrap());
        let data_segment = buf.pop_first().unwrap();
        assert_eq!(data_segment.start_sequence(), Sequence::new(0));
        assert_eq!(buf.next, Sequence::new(1));
        let data_segment = buf.pop_first().unwrap();
        assert_eq!(data_segment.start_sequence(), Sequence::new(1));
        assert_eq!(buf.next, Sequence::new(2));
        let data_segment = buf.pop_first().unwrap();
        assert_eq!(data_segment.start_sequence(), Sequence::new(2));
        assert_eq!(buf.next, Sequence::new(3));
        assert!(buf.pop_first().is_none());
    }

    #[test]
    fn remove_stale_data() {
        let mut buf = RecvStreamBuf::new();
        buf.insert(DataSegment::new(Sequence::new(0), Bytes::from_iter(vec![0, 1, 2])).unwrap());
        let _ = buf.pop_first().unwrap();
        buf.insert(
            DataSegment::new(Sequence::new(0), Bytes::from_iter(vec![0, 1, 2, 3, 4])).unwrap(),
        );
        let data_segment = buf.pop_first().unwrap();
        assert_eq!(data_segment.start_sequence(), Sequence::new(3));
        assert_eq!(data_segment.size(), 2);
        assert_eq!(buf.next, data_segment.end_sequence());
        assert!(buf.pop_first().is_none());
    }

    #[test]
    fn deduplicate_1() {
        let mut buf = RecvStreamBuf::new();
        buf.insert(DataSegment::new(Sequence::new(0), Bytes::from_iter(vec![0, 1, 2])).unwrap());
        buf.insert(DataSegment::new(Sequence::new(1), Bytes::from_iter(vec![1, 2])).unwrap());
        let _ = buf.pop_first().unwrap();
        assert_eq!(buf.next, Sequence::new(3));
        assert!(buf.pop_first().is_none());
    }

    #[test]
    fn deduplicate_2() {
        let mut buf = RecvStreamBuf::new();
        buf.insert(DataSegment::new(Sequence::new(0), Bytes::from_iter(vec![0, 1, 2])).unwrap());
        buf.insert(DataSegment::new(Sequence::new(1), Bytes::from_iter(vec![1, 2, 3])).unwrap());
        let _ = buf.pop_first().unwrap();
        assert_eq!(buf.next, Sequence::new(3));
        let data_segment = buf.pop_first().unwrap();
        assert_eq!(data_segment.size(), 1);
        assert_eq!(data_segment.start_sequence(), Sequence::new(3));
        assert_eq!(buf.next, Sequence::new(4));
        assert!(buf.pop_first().is_none());
    }
}
