//! End-to-end conformance for the Kafka adapter over real TCP sockets
//! (spec.txt §6 Phase 3, the in-repo stand-in for the librdkafka harness until
//! that lands). A minimal hand-rolled Kafka client drives produce/fetch,
//! metadata, and consumer-group offset commit/fetch against the broker.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use synapse_adapter_kafka::server::serve_with_listener;
use synapse_adapter_kafka::{ApiKey, Broker};
use synapse_core::SynapseCore;

/// Big-endian slice readers tolerant of a partial buffer (`None` = need more).
mod rd {
    pub fn i32(b: &[u8], p: usize) -> Option<i32> {
        if b.len() >= p + 4 {
            Some(i32::from_be_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]))
        } else {
            None
        }
    }
    pub fn i16(b: &[u8], p: usize) -> Option<i16> {
        if b.len() >= p + 2 {
            Some(i16::from_be_bytes([b[p], b[p + 1]]))
        } else {
            None
        }
    }
    pub fn i64(b: &[u8], p: usize) -> Option<i64> {
        if b.len() >= p + 8 {
            let mut a = [0u8; 8];
            a.copy_from_slice(&b[p..p + 8]);
            Some(i64::from_be_bytes(a))
        } else {
            None
        }
    }
    pub fn str(b: &[u8], p: usize) -> Option<(String, usize)> {
        let len = i16(b, p)?;
        if len < 0 {
            return Some((String::new(), p + 2));
        }
        let l = len as usize;
        if b.len() < p + 2 + l {
            return None;
        }
        let s = String::from_utf8_lossy(&b[p + 2..p + 2 + l]).into_owned();
        Some((s, p + 2 + l))
    }
}

/// Cursor over a response body; advances as fields are consumed.
struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn i32(&mut self) -> Option<i32> {
        let v = rd::i32(self.b, self.p)?;
        self.p += 4;
        Some(v)
    }
    fn i16(&mut self) -> Option<i16> {
        let v = rd::i16(self.b, self.p)?;
        self.p += 2;
        Some(v)
    }
    fn i64(&mut self) -> Option<i64> {
        let v = rd::i64(self.b, self.p)?;
        self.p += 8;
        Some(v)
    }
    fn str(&mut self) -> Option<String> {
        let (s, np) = rd::str(self.b, self.p)?;
        self.p = np;
        Some(s)
    }
    fn i8_byte(&mut self) -> Option<u8> {
        if self.p < self.b.len() {
            let v = self.b[self.p];
            self.p += 1;
            Some(v)
        } else {
            None
        }
    }
}

fn put_i16(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_i8(out: &mut Vec<u8>, v: i8) {
    out.push(v as u8);
}
fn put_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => put_i16(out, -1),
        Some(s) => {
            put_i16(out, s.len() as i16);
            out.extend_from_slice(s.as_bytes());
        }
    }
}
fn put_bytes(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => put_i32(out, -1),
        Some(b) => {
            put_i32(out, b.len() as i32);
            out.extend_from_slice(b);
        }
    }
}

/// Build one Kafka request frame (size prefix + header v1 + body).
fn request_frame(api_key: i16, correlation: i32, client_id: &str, body: &[u8]) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend_from_slice(&api_key.to_be_bytes());
    inner.extend_from_slice(&1i16.to_be_bytes()); // api version 1 (server strips client_id)
    inner.extend_from_slice(&correlation.to_be_bytes());
    put_str(&mut inner, Some(client_id));
    inner.extend_from_slice(body);
    let mut frame = Vec::new();
    frame.extend_from_slice(&(inner.len() as i32).to_be_bytes());
    frame.extend_from_slice(&inner);
    frame
}

/// Read a full Kafka response (correlation + body) off the socket.
async fn read_response(sock: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 65536];
    loop {
        match timeout(Duration::from_millis(200), sock.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
            Ok(Err(_)) => break,
            Err(_) => {
                if !buf.is_empty() {
                    break;
                }
            }
        }
    }
    buf
}

async fn connect() -> TcpStream {
    let broker = Arc::new(Broker::new(Arc::new(SynapseCore::new())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_with_listener(broker, listener));
    TcpStream::connect(addr).await.unwrap()
}

#[tokio::test]
async fn api_versions_and_produce_fetch() {
    let mut sock = connect().await;

    // ApiVersions
    let req = request_frame(ApiKey::ApiVersions.as_i16(), 1, "c", &[]);
    sock.write_all(&req).await.unwrap();
    let resp = read_response(&mut sock).await;
    let body = &resp[4..];
    assert_eq!(Cur { b: body, p: 0 }.i16().unwrap(), 0); // error code

    // Produce "test"/0 = "hello"
    let mut pbody = Vec::new();
    put_i16(&mut pbody, 1); // acks
    put_i32(&mut pbody, 1000); // timeout
    put_i32(&mut pbody, 1); // topics
    put_str(&mut pbody, Some("test"));
    put_i32(&mut pbody, 1); // partitions
    put_i32(&mut pbody, 0); // partition 0
    put_bytes(&mut pbody, Some(b"hello"));
    let preq = request_frame(ApiKey::Produce.as_i16(), 2, "c", &pbody);
    sock.write_all(&preq).await.unwrap();
    let presp = read_response(&mut sock).await;
    let mut c = Cur {
        b: &presp[4..],
        p: 0,
    };
    let ntopics = c.i32().unwrap();
    assert_eq!(ntopics, 1);
    let _topic = c.str().unwrap();
    let nparts = c.i32().unwrap();
    assert_eq!(nparts, 1);
    let _part = c.i32().unwrap();
    let err = c.i16().unwrap();
    let offset = c.i64().unwrap();
    assert_eq!(err, 0);
    assert_eq!(offset, 0);

    // Fetch "test"/0 from offset 0
    let mut fbody = Vec::new();
    put_i32(&mut fbody, -1); // replica_id
    put_i32(&mut fbody, 0); // max_wait
    put_i32(&mut fbody, 1); // min_bytes
    put_i32(&mut fbody, 1); // topics
    put_str(&mut fbody, Some("test"));
    put_i32(&mut fbody, 1); // partitions
    put_i32(&mut fbody, 0); // partition
    put_i64(&mut fbody, 0); // fetch_offset
    put_i32(&mut fbody, 1024); // max_bytes
    let freq = request_frame(ApiKey::Fetch.as_i16(), 3, "c", &fbody);
    sock.write_all(&freq).await.unwrap();
    let fresp = read_response(&mut sock).await;
    let fbody = &fresp[4..];
    assert!(fbody.windows(5).any(|w| w == b"hello"), "fetch returned payload");
}

#[tokio::test]
async fn consumer_group_offsets() {
    let mut sock = connect().await;

    // FindCoordinator for the group.
    let mut cbody = Vec::new();
    put_str(&mut cbody, Some("g1"));
    put_i8(&mut cbody, 0);
    let creq = request_frame(ApiKey::FindCoordinator.as_i16(), 1, "c", &cbody);
    sock.write_all(&creq).await.unwrap();
    let cresp = read_response(&mut sock).await;
    assert_eq!(Cur { b: &cresp[4..], p: 0 }.i16().unwrap(), 0); // error

    // OffsetCommit 42 on test/0
    let mut obody = Vec::new();
    put_str(&mut obody, Some("g1"));
    put_i32(&mut obody, 1); // topics
    put_str(&mut obody, Some("test"));
    put_i32(&mut obody, 1); // partitions
    put_i32(&mut obody, 0); // partition
    put_i64(&mut obody, 42); // offset
    put_str(&mut obody, Some("")); // metadata
    let oreq = request_frame(ApiKey::OffsetCommit.as_i16(), 2, "c", &obody);
    sock.write_all(&oreq).await.unwrap();
    let oresp = read_response(&mut sock).await;
    let mut c = Cur {
        b: &oresp[4..],
        p: 0,
    };
    let ntopics = c.i32().unwrap();
    let _topic = c.str().unwrap();
    let nparts = c.i32().unwrap();
    assert_eq!((ntopics, nparts), (1, 1));
    let _part = c.i32().unwrap();
    assert_eq!(c.i16().unwrap(), 0); // partition error

    // OffsetFetch returns 42.
    let mut fbody = Vec::new();
    put_str(&mut fbody, Some("g1"));
    put_i32(&mut fbody, 1); // topics
    put_str(&mut fbody, Some("test"));
    put_i32(&mut fbody, 1); // partitions
    put_i32(&mut fbody, 0);
    let freq = request_frame(ApiKey::OffsetFetch.as_i16(), 3, "c", &fbody);
    sock.write_all(&freq).await.unwrap();
    let fresp = read_response(&mut sock).await;
    let mut c = Cur {
        b: &fresp[4..],
        p: 0,
    };
    let _ntopics = c.i32().unwrap();
    let _topic = c.str().unwrap();
    let _nparts = c.i32().unwrap();
    let _part = c.i32().unwrap();
    let offset = c.i64().unwrap();
    let _meta = c.str().unwrap();
    let err = c.i16().unwrap();
    assert_eq!(offset, 42);
    assert_eq!(err, 0);
}

#[tokio::test]
async fn metadata_lists_topic() {
    let mut sock = connect().await;

    // Produce first so the topic is registered.
    let mut pbody = Vec::new();
    put_i16(&mut pbody, 1);
    put_i32(&mut pbody, 1000);
    put_i32(&mut pbody, 1);
    put_str(&mut pbody, Some("events"));
    put_i32(&mut pbody, 1);
    put_i32(&mut pbody, 0);
    put_bytes(&mut pbody, Some(b"x"));
    let preq = request_frame(ApiKey::Produce.as_i16(), 1, "c", &pbody);
    sock.write_all(&preq).await.unwrap();
    read_response(&mut sock).await;

    // Metadata (null topic list => all topics).
    let mut mb = Vec::new();
    put_i32(&mut mb, -1); // null array => all topics
    let mreq = request_frame(ApiKey::Metadata.as_i16(), 2, "c", &mb);
    sock.write_all(&mreq).await.unwrap();
    let mresp = read_response(&mut sock).await;
    let mut c = Cur {
        b: &mresp[4..],
        p: 0,
    };
    let nbrokers = c.i32().unwrap();
    for _ in 0..nbrokers {
        let _node = c.i32().unwrap();
        let _host = c.str().unwrap();
        let _port = c.i32().unwrap();
    }
    let ntopics = c.i32().unwrap();
    assert!(ntopics >= 1);
    let mut found = false;
    for _ in 0..ntopics {
        let _err = c.i16().unwrap();
        let name = c.str().unwrap();
        let _internal = c.i8_byte();
        let nparts = c.i32().unwrap();
        for _ in 0..nparts {
            let _perr = c.i16().unwrap();
            let _pid = c.i32().unwrap();
            let _leader = c.i32().unwrap();
            let nreplicas = c.i32().unwrap();
            for _ in 0..nreplicas {
                let _r = c.i32().unwrap();
            }
            let nisr = c.i32().unwrap();
            for _ in 0..nisr {
                let _r = c.i32().unwrap();
            }
        }
        if name == "events" {
            found = true;
        }
    }
    assert!(found, "metadata should include produced topic");
}
