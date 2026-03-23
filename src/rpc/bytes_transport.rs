// We fork capnp-rpc's twoparty read path to read into Bytes (refcounted)
// instead of Vec<Word> (owned, aligned). This enables zero-copy slice_ref
// sub-slicing of key/value data:
//   - Produce server: incoming Call messages → handler pops and slice_ref's
//     key/value from the request.
//   - Consume client: incoming Return messages → client pops and slice_ref's
//     key/value from the response.
// Cost:
//   - ~280 lines mirroring capnp-rpc internals (pinned to 0.25 API)
//   - requiring capnp's "unaligned" feature (slightly slower field reads)
//   - implicit ordering assumption on the per-connection VecDeque

use bytes::{Bytes, BytesMut};
use capnp::capability::Promise;
use capnp::message::ReaderOptions;
use capnp::serialize::BufferSegments;
use capnp_rpc::rpc_capnp;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, FutureExt, TryFutureExt};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

use futures::channel::oneshot;

pub type VatId = capnp_rpc::rpc_twoparty_capnp::Side;

/// Per-connection queue of raw `Bytes` for RPC messages. The transport
/// pushes; the handler/client pops.
pub type MessageBytesQueue = Rc<RefCell<VecDeque<Bytes>>>;

// ---------------------------------------------------------------------------
// Incoming message backed by Bytes
// ---------------------------------------------------------------------------

struct BytesIncomingMessage {
    reader: capnp::message::Reader<BufferSegments<Bytes>>,
}

impl capnp_rpc::IncomingMessage for BytesIncomingMessage {
    fn get_body(&self) -> capnp::Result<capnp::any_pointer::Reader<'_>> {
        self.reader.get_root()
    }
}

// ---------------------------------------------------------------------------
// Read a capnp message from an AsyncRead into Bytes
// ---------------------------------------------------------------------------

async fn try_read_message_bytes<R>(
    mut reader: R,
    options: ReaderOptions,
) -> capnp::Result<Option<Bytes>>
where
    R: AsyncRead + Unpin,
{
    let mut first_word = [0u8; 8];
    {
        let n = reader.read(&mut first_word[..]).await?;
        if n == 0 {
            return Ok(None);
        }
        if n < 8 {
            reader.read_exact(&mut first_word[n..]).await?;
        }
    }

    let segment_count =
        u32::from_le_bytes(first_word[0..4].try_into().unwrap()).wrapping_add(1) as usize;
    if segment_count >= 512 || segment_count == 0 {
        return Err(capnp::Error::failed(format!(
            "bad segment count: {segment_count}"
        )));
    }

    // Segment table size in bytes (including the first word we already read).
    // The table is: 4 bytes count + 4*segment_count segment lengths, padded
    // to an 8-byte boundary.
    let table_words = (segment_count / 2) + 1; // ceil((4 + 4*segment_count) / 8)
    let table_bytes = table_words * 8;
    let remaining_table_bytes = table_bytes - 8; // we already read the first 8

    // Read the rest of the segment table into a temp buffer so we can parse
    // segment lengths.
    let mut table_rest = vec![0u8; remaining_table_bytes];
    if remaining_table_bytes > 0 {
        reader.read_exact(&mut table_rest).await?;
    }

    // Parse segment lengths.
    let first_seg_words = u32::from_le_bytes(first_word[4..8].try_into().unwrap()) as usize;
    let mut total_segment_words = first_seg_words;
    for idx in 1..segment_count {
        let off = (idx - 1) * 4;
        let seg_words = u32::from_le_bytes(table_rest[off..off + 4].try_into().unwrap()) as usize;
        total_segment_words += seg_words;
    }

    if let Some(limit) = options.traversal_limit_in_words
        && total_segment_words > limit
    {
        return Err(capnp::Error::failed(format!(
            "message has {total_segment_words} words, exceeding traversal limit"
        )));
    }

    let segment_data_bytes = total_segment_words * 8;
    let total_bytes = table_bytes + segment_data_bytes;

    let mut buf = BytesMut::with_capacity(total_bytes);
    buf.extend_from_slice(&first_word);
    buf.extend_from_slice(&table_rest);
    buf.resize(total_bytes, 0);
    reader.read_exact(&mut buf[table_bytes..]).await?;

    Ok(Some(buf.freeze()))
}

fn is_call_message(bytes: &Bytes, options: ReaderOptions) -> bool {
    let Ok(segments) = BufferSegments::new(bytes.clone(), options) else {
        return false;
    };
    let reader = capnp::message::Reader::new(segments, options);
    let Ok(body) = reader.get_root::<rpc_capnp::message::Reader<'_>>() else {
        return false;
    };
    matches!(body.which(), Ok(rpc_capnp::message::Call(_)))
}

fn is_return_message(bytes: &Bytes, options: ReaderOptions) -> bool {
    let Ok(segments) = BufferSegments::new(bytes.clone(), options) else {
        return false;
    };
    let reader = capnp::message::Reader::new(segments, options);
    let Ok(body) = reader.get_root::<rpc_capnp::message::Reader<'_>>() else {
        return false;
    };
    matches!(body.which(), Ok(rpc_capnp::message::Return(_)))
}

// ---------------------------------------------------------------------------
// Outgoing message (delegates to capnp_futures::Sender, unchanged)
// ---------------------------------------------------------------------------

struct OutgoingMessage {
    message: capnp::message::Builder<capnp::message::HeapAllocator>,
    sender: capnp_futures::Sender<Rc<capnp::message::Builder<capnp::message::HeapAllocator>>>,
}

impl capnp_rpc::OutgoingMessage for OutgoingMessage {
    fn get_body(&mut self) -> capnp::Result<capnp::any_pointer::Builder<'_>> {
        self.message.get_root()
    }

    fn get_body_as_reader(&self) -> capnp::Result<capnp::any_pointer::Reader<'_>> {
        self.message.get_root_as_reader()
    }

    fn send(
        self: Box<Self>,
    ) -> (
        Promise<(), capnp::Error>,
        Rc<capnp::message::Builder<capnp::message::HeapAllocator>>,
    ) {
        let tmp = *self;
        let OutgoingMessage {
            message,
            mut sender,
        } = tmp;
        let m = Rc::new(message);
        (
            Promise::from_future(sender.send(m.clone()).map_ok(|_| ())),
            m,
        )
    }

    fn take(self: Box<Self>) -> capnp::message::Builder<capnp::message::HeapAllocator> {
        self.message
    }

    fn size_in_words(&self) -> usize {
        self.message.size_in_words()
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

struct ConnectionInner<T: AsyncRead + 'static> {
    input_stream: Rc<RefCell<Option<T>>>,
    sender: capnp_futures::Sender<Rc<capnp::message::Builder<capnp::message::HeapAllocator>>>,
    side: VatId,
    receive_options: ReaderOptions,
    on_disconnect_fulfiller: Option<oneshot::Sender<()>>,
    call_bytes_queue: Option<MessageBytesQueue>,
    return_bytes_queue: Option<MessageBytesQueue>,
}

impl<T: AsyncRead> Drop for ConnectionInner<T> {
    fn drop(&mut self) {
        if let Some(f) = self.on_disconnect_fulfiller.take() {
            let _ = f.send(());
        }
    }
}

struct Connection<T: AsyncRead + 'static> {
    inner: Rc<RefCell<ConnectionInner<T>>>,
}

impl<T: AsyncRead + Unpin> capnp_rpc::Connection<VatId> for Connection<T> {
    fn get_peer_vat_id(&self) -> VatId {
        self.inner.borrow().side
    }

    fn new_outgoing_message(
        &mut self,
        first_segment_word_size: u32,
    ) -> Box<dyn capnp_rpc::OutgoingMessage> {
        let message = capnp::message::Builder::new(
            capnp::message::HeapAllocator::new().first_segment_words(first_segment_word_size),
        );
        Box::new(OutgoingMessage {
            message,
            sender: self.inner.borrow().sender.clone(),
        })
    }

    fn receive_incoming_message(
        &mut self,
    ) -> Promise<Option<Box<dyn capnp_rpc::IncomingMessage + 'static>>, capnp::Error> {
        let inner = self.inner.borrow_mut();
        let maybe_input_stream = inner.input_stream.borrow_mut().take();
        let return_it_here = inner.input_stream.clone();
        let receive_options = inner.receive_options;
        let call_queue = inner.call_bytes_queue.clone();
        let return_queue = inner.return_bytes_queue.clone();

        match maybe_input_stream {
            Some(mut s) => Promise::from_future(async move {
                let maybe_bytes = try_read_message_bytes(&mut s, receive_options).await?;
                *return_it_here.borrow_mut() = Some(s);
                match maybe_bytes {
                    None => Ok(None),
                    Some(bytes) => {
                        if let Some(ref q) = call_queue
                            && is_call_message(&bytes, receive_options)
                        {
                            q.borrow_mut().push_back(bytes.clone());
                        }
                        if let Some(ref q) = return_queue
                            && is_return_message(&bytes, receive_options)
                        {
                            q.borrow_mut().push_back(bytes.clone());
                        }
                        let segments = BufferSegments::new(bytes, receive_options)?;
                        let reader = capnp::message::Reader::new(segments, receive_options);
                        Ok(Some(Box::new(BytesIncomingMessage { reader })
                            as Box<dyn capnp_rpc::IncomingMessage>))
                    }
                }
            }),
            None => Promise::err(capnp::Error::failed(
                "input stream already taken".to_string(),
            )),
        }
    }

    fn shutdown(&mut self, result: capnp::Result<()>) -> Promise<(), capnp::Error> {
        Promise::from_future(self.inner.borrow_mut().sender.terminate(result))
    }
}

// ---------------------------------------------------------------------------
// VatNetwork
// ---------------------------------------------------------------------------

pub struct BytesVatNetwork<T: AsyncRead + 'static + Unpin> {
    connection: Option<Connection<T>>,
    weak_connection_inner: Weak<RefCell<ConnectionInner<T>>>,
    execution_driver: futures::future::Shared<Promise<(), capnp::Error>>,
    side: VatId,
}

impl<T: AsyncRead + Unpin> BytesVatNetwork<T> {
    pub fn new<U>(
        input_stream: T,
        output_stream: U,
        side: VatId,
        receive_options: ReaderOptions,
        call_bytes_queue: Option<MessageBytesQueue>,
        return_bytes_queue: Option<MessageBytesQueue>,
    ) -> Self
    where
        U: AsyncWrite + 'static + Unpin,
    {
        let (fulfiller, disconnect_promise) = oneshot::channel();
        let disconnect_promise =
            disconnect_promise.map_err(|_| capnp::Error::disconnected("disconnected".into()));

        let (execution_driver, sender) = {
            let (tx, write_queue) = capnp_futures::write_queue(output_stream);
            (
                Promise::from_future(write_queue.then(move |r| {
                    disconnect_promise
                        .then(move |_| futures::future::ready(r))
                        .map_ok(|_| ())
                }))
                .shared(),
                tx,
            )
        };

        let inner = Rc::new(RefCell::new(ConnectionInner {
            input_stream: Rc::new(RefCell::new(Some(input_stream))),
            sender,
            side,
            receive_options,
            on_disconnect_fulfiller: Some(fulfiller),
            call_bytes_queue,
            return_bytes_queue,
        }));

        let weak_inner = Rc::downgrade(&inner);

        Self {
            connection: Some(Connection { inner }),
            weak_connection_inner: weak_inner,
            execution_driver,
            side,
        }
    }
}

impl<T: AsyncRead + Unpin> capnp_rpc::VatNetwork<VatId> for BytesVatNetwork<T> {
    fn connect(&mut self, host_id: VatId) -> Option<Box<dyn capnp_rpc::Connection<VatId>>> {
        if host_id == self.side {
            None
        } else {
            match self.weak_connection_inner.upgrade() {
                Some(inner) => Some(Box::new(Connection { inner })),
                None => panic!("tried to reconnect a disconnected vat network"),
            }
        }
    }

    fn accept(&mut self) -> Promise<Box<dyn capnp_rpc::Connection<VatId>>, capnp::Error> {
        match self.connection.take() {
            Some(c) => Promise::ok(Box::new(c) as Box<dyn capnp_rpc::Connection<VatId>>),
            None => Promise::from_future(futures::future::pending()),
        }
    }

    fn drive_until_shutdown(&mut self) -> Promise<(), capnp::Error> {
        Promise::from_future(self.execution_driver.clone())
    }
}
