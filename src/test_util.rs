//! Shared test helpers.

use bytes::Bytes;

use crate::rpc::barka_capnp::produce_request;

/// Holds a `Bytes`-backed capnp message reader, keeping the parsed segments
/// alive so we can borrow a `produce_request::Reader` from it.
pub struct TestMessage {
    bytes: Bytes,
    reader: capnp::message::Reader<capnp::serialize::BufferSegments<Bytes>>,
}

impl TestMessage {
    fn build(n_records: usize, value_fn: impl Fn(usize) -> Vec<u8>) -> Self {
        let mut builder = capnp::message::Builder::new_default();
        {
            let mut req = builder.init_root::<produce_request::Builder>();
            req.set_topic("test");
            req.set_partition(0);
            let mut records = req.init_records(n_records as u32);
            for i in 0..n_records {
                let mut r = records.reborrow().get(i as u32);
                r.set_key(format!("k{i}").as_bytes());
                r.set_value(&value_fn(i));
                r.set_timestamp(i as i64);
            }
        }
        let words = capnp::serialize::write_message_to_words(&builder);
        let bytes = Bytes::from(words);
        let segments =
            capnp::serialize::BufferSegments::new(bytes.clone(), Default::default()).unwrap();
        let reader = capnp::message::Reader::new(segments, Default::default());
        Self { bytes, reader }
    }

    /// Build a produce request with `n_records` records whose values are
    /// `"{value_prefix}-{i}"` and keys are `"k{i}"`.
    pub fn new(n_records: usize, value_prefix: &str) -> Self {
        let prefix = value_prefix.to_string();
        Self::build(n_records, move |i| format!("{prefix}-{i}").into_bytes())
    }

    /// Build a produce request with `n_records` records whose values are
    /// all `value_size` bytes of `'x'`.
    pub fn with_value_size(n_records: usize, value_size: usize) -> Self {
        Self::build(n_records, move |_| vec![b'x'; value_size])
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn request(&self) -> produce_request::Reader<'_> {
        self.reader
            .get_root::<produce_request::Reader<'_>>()
            .unwrap()
    }
}
