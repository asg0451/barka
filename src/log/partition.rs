use crate::log::record::Record;

pub struct Partition {
    pub id: u32,
    next_offset: u64,
    buffer: Vec<Record>,
}

impl Partition {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            next_offset: 0,
            buffer: Vec::new(),
        }
    }

    pub fn append(&mut self, key: Option<Vec<u8>>, value: Vec<u8>, timestamp: i64) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;
        self.buffer.push(Record {
            key,
            value,
            offset,
            timestamp,
        });
        offset
    }

    pub fn read(&self, offset: u64, max: u32) -> Vec<Record> {
        self.buffer
            .iter()
            .filter(|r| r.offset >= offset)
            .take(max as usize)
            .cloned()
            .collect()
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
}
