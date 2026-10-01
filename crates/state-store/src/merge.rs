use crate::{invalid, Record};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;

pub(crate) type Source = Box<dyn Iterator<Item = io::Result<Record>>>;
struct Head {
    source: usize,
    record: Record,
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .key
            .cmp(&self.record.key)
            .then(self.record.sequence.cmp(&other.record.sequence))
            .then(other.source.cmp(&self.source))
    }
}

/// Each heap item retains its source, including after equal-key runs merge.
pub(crate) struct Merge {
    sources: Vec<Source>,
    heap: BinaryHeap<Head>,
    failed: bool,
}
impl Merge {
    pub fn new(sources: Vec<Source>) -> io::Result<Self> {
        let mut result = Self {
            sources,
            heap: BinaryHeap::new(),
            failed: false,
        };
        for source in 0..result.sources.len() {
            result.advance(source)?;
        }
        Ok(result)
    }
    fn advance(&mut self, source: usize) -> io::Result<()> {
        if let Some(record) = self.sources[source].next() {
            self.heap.push(Head {
                source,
                record: record?,
            });
        }
        Ok(())
    }
    fn take(&mut self) -> io::Result<Option<Record>> {
        let Some(head) = self.heap.pop() else {
            return Ok(None);
        };
        let winner = head.record;
        self.advance(head.source)?;
        while self
            .heap
            .peek()
            .is_some_and(|head| head.record.key == winner.key)
        {
            let duplicate = self.heap.pop().expect("peeked duplicate");
            if duplicate.record.sequence > winner.sequence
                || (duplicate.record.sequence == winner.sequence
                    && duplicate.record.value != winner.value)
            {
                return Err(invalid(
                    "conflicting equal-sequence record or unsorted source",
                ));
            }
            self.advance(duplicate.source)?;
        }
        Ok(Some(winner))
    }
}
impl Iterator for Merge {
    type Item = io::Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.take() {
            Ok(record) => record.map(Ok),
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(key: u8, value: u8, sequence: u64) -> Record {
        Record {
            key: vec![key],
            value: Some(vec![value]),
            sequence,
        }
    }
    #[test]
    fn equal_keys_advance_the_original_run_and_choose_the_newest() {
        let runs = vec![
            vec![row(1, 1, 1), row(3, 1, 1)],
            vec![row(1, 2, 2), row(2, 2, 2)],
            vec![row(1, 3, 3), row(4, 3, 3)],
        ];
        let sources = runs
            .into_iter()
            .map(|run| Box::new(run.into_iter().map(Ok)) as Source)
            .collect();
        let rows = Merge::new(sources)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![row(1, 3, 3), row(2, 2, 2), row(3, 1, 1), row(4, 3, 3)]
        );
    }
    #[test]
    fn contradictory_same_version_is_corruption() {
        let sources = vec![
            Box::new(vec![Ok(row(1, 1, 1))].into_iter()) as Source,
            Box::new(vec![Ok(row(1, 2, 1))].into_iter()) as Source,
        ];
        assert!(Merge::new(sources).unwrap().next().unwrap().is_err());
    }
}
