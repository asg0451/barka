//! Binary segment format optimised for scatter-gather uploads.
//!
//! ```text
//! [8 bytes]  epoch: u64 LE
//! [4 bytes]  record_count: u32 LE
//! Per record (metadata, contiguous):
//!   [8 bytes]  offset: u64 LE
//!   [8 bytes]  timestamp: i64 LE
//!   [4 bytes]  key_len: u32 LE
//!   [4 bytes]  value_len: u32 LE
//! Per record (data, contiguous):
//!   [key_len bytes]   key data
//!   [value_len bytes] value data
//! ```

use bytes::Bytes;

/// Data for a single record, with zero-copy key/value slices.
#[derive(Clone, Debug)]
pub struct RecordData {
    pub offset: u64,
    pub timestamp: i64,
    pub key: Bytes,
    pub value: Bytes,
}

const HEADER_BYTES: usize = 8 + 4; // epoch + record_count
const PER_RECORD_META: usize = 8 + 8 + 4 + 4; // offset + timestamp + key_len + value_len

/// Build a scatter-gather list of `Bytes` chunks ready for streaming upload.
///
/// Returns `(chunks, total_content_length)`. The header + per-record metadata
/// is a single small allocation; key and value chunks are zero-copy references
/// into the original RPC message buffers.
pub fn encode_gather(epoch: u64, records: &[RecordData]) -> (Vec<Bytes>, u64) {
    let meta_size = HEADER_BYTES + records.len() * PER_RECORD_META;
    let mut meta = Vec::with_capacity(meta_size);

    meta.extend_from_slice(&epoch.to_le_bytes());
    meta.extend_from_slice(&(records.len() as u32).to_le_bytes());

    let mut data_len: u64 = 0;
    for rec in records {
        meta.extend_from_slice(&rec.offset.to_le_bytes());
        meta.extend_from_slice(&rec.timestamp.to_le_bytes());
        meta.extend_from_slice(&(rec.key.len() as u32).to_le_bytes());
        meta.extend_from_slice(&(rec.value.len() as u32).to_le_bytes());
        data_len += rec.key.len() as u64 + rec.value.len() as u64;
    }

    let total = meta.len() as u64 + data_len;
    let mut chunks = Vec::with_capacity(1 + records.len() * 2);
    chunks.push(Bytes::from(meta));
    for rec in records {
        if !rec.key.is_empty() {
            chunks.push(rec.key.clone());
        }
        if !rec.value.is_empty() {
            chunks.push(rec.value.clone());
        }
    }
    (chunks, total)
}

/// Decode a segment from contiguous bytes (for consumers / tests).
pub fn decode(data: &[u8]) -> anyhow::Result<(u64, Vec<RecordData>)> {
    anyhow::ensure!(data.len() >= HEADER_BYTES, "segment too short for header");

    let epoch = u64::from_le_bytes(data[0..8].try_into()?);
    let count = u32::from_le_bytes(data[8..12].try_into()?) as usize;

    let meta_end = HEADER_BYTES + count * PER_RECORD_META;
    anyhow::ensure!(data.len() >= meta_end, "segment too short for record metadata");

    let mut records = Vec::with_capacity(count);
    let mut cursor = meta_end;

    for i in 0..count {
        let base = HEADER_BYTES + i * PER_RECORD_META;
        let offset = u64::from_le_bytes(data[base..base + 8].try_into()?);
        let timestamp = i64::from_le_bytes(data[base + 8..base + 16].try_into()?);
        let key_len = u32::from_le_bytes(data[base + 16..base + 20].try_into()?) as usize;
        let value_len = u32::from_le_bytes(data[base + 20..base + 24].try_into()?) as usize;

        anyhow::ensure!(
            cursor + key_len + value_len <= data.len(),
            "segment too short for record data"
        );

        let key = Bytes::copy_from_slice(&data[cursor..cursor + key_len]);
        cursor += key_len;
        let value = Bytes::copy_from_slice(&data[cursor..cursor + value_len]);
        cursor += value_len;

        records.push(RecordData {
            offset,
            timestamp,
            key,
            value,
        });
    }

    Ok((epoch, records))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let (chunks, len) = encode_gather(42, &[]);
        let buf: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(buf.len() as u64, len);
        let (epoch, records) = decode(&buf).unwrap();
        assert_eq!(epoch, 42);
        assert!(records.is_empty());
    }

    #[test]
    fn round_trip_records() {
        let records = vec![
            RecordData {
                offset: 0,
                timestamp: 1000,
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            RecordData {
                offset: 1,
                timestamp: 2000,
                key: Bytes::new(),
                value: Bytes::from_static(b"val-only"),
            },
        ];

        let (chunks, len) = encode_gather(7, &records);
        let buf: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(buf.len() as u64, len);

        let (epoch, decoded) = decode(&buf).unwrap();
        assert_eq!(epoch, 7);
        assert_eq!(decoded.len(), 2);

        assert_eq!(decoded[0].offset, 0);
        assert_eq!(decoded[0].timestamp, 1000);
        assert_eq!(decoded[0].key.as_ref(), b"k1");
        assert_eq!(decoded[0].value.as_ref(), b"v1");

        assert_eq!(decoded[1].offset, 1);
        assert_eq!(decoded[1].timestamp, 2000);
        assert!(decoded[1].key.is_empty());
        assert_eq!(decoded[1].value.as_ref(), b"val-only");
    }
}
