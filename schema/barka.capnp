@0xa8e4f56b3c1d9e70;

struct Record {
  key       @0 :Data;
  value     @1 :Data;
  offset    @2 :UInt64;
  timestamp @3 :Int64;
}

struct ProduceRequest {
  topic     @0 :Text;
  partition @1 :UInt32;
  records   @2 :List(Record);
}

# Server-assigned fields only — no key/value echo (avoids copying payload into
# the response message; clients merge with their produce request).
struct ProducedRecordMeta {
  offset    @0 :UInt64;
  timestamp @1 :Int64;
}

struct ProduceResponse {
  records @0 :List(ProducedRecordMeta);
}

struct ConsumeRequest {
  topic      @0 :Text;
  partition  @1 :UInt32;
  offset     @2 :UInt64;
  maxRecords @3 :UInt32;
}

struct ConsumeResponse {
  records @0 :List(Record);
}

struct SegmentData {
  epoch   @0 :UInt64;
  records @1 :List(Record);
}

interface BarkaSvc {
  produce @0 (request :ProduceRequest) -> (response :ProduceResponse);
  consume @1 (request :ConsumeRequest) -> (response :ConsumeResponse);
}
