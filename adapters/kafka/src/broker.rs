//! Kafka broker: produce/fetch → core [`Log`] writes/reads, and consumer-group
//! offsets/rebalancing via the routing [`StreamRouter`] (spec.txt §3.2, §6
//! Phase 3). The broker is transport-agnostic; [`crate::server`] wires it to a
//! TCP listener, reusing the same per-connection mpsc model as the other
//! adapters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use synapse_core::SynapseCore;
use synapse_routing::stream::StreamRouter;

use crate::codec::{
    encode_api_versions, encode_error_only, encode_fetch, encode_find_coordinator,
    encode_join_group, encode_list_offsets, encode_metadata, encode_offset_commit,
    encode_offset_fetch, encode_produce, encode_sync_group, parse_fetch, parse_find_coordinator,
    parse_join_group, parse_list_offsets, parse_member_request, parse_metadata,
    parse_offset_commit, parse_offset_fetch, parse_produce, parse_sync_group, ApiKey, Frame,
    ERR_COORDINATOR_NOT_AVAILABLE, ERR_NONE, ERR_UNKNOWN, ERR_UNKNOWN_TOPIC_OR_PARTITION,
};

const TENANT: &str = "kafka";
const DEFAULT_PORT: i32 = 9092;

#[derive(Debug, Default)]
struct GroupState {
    generation: i32,
    members: Vec<String>,
    leader: Option<String>,
    assignments: HashMap<String, Vec<u8>>,
}

/// Shared Kafka broker state.
pub struct Broker {
    core: Arc<SynapseCore>,
    router: Arc<StreamRouter>,
    topics: Mutex<HashMap<String, u32>>,
    groups: Mutex<HashMap<String, GroupState>>,
    next_member: AtomicU32,
}

fn log_name(topic: &str, partition: i32) -> String {
    format!("{topic}-{partition}")
}

impl Broker {
    pub fn new(core: Arc<SynapseCore>) -> Self {
        Self {
            core,
            router: Arc::new(StreamRouter::new()),
            topics: Mutex::new(HashMap::new()),
            groups: Mutex::new(HashMap::new()),
            next_member: AtomicU32::new(1),
        }
    }

    fn ensure_topic(&self, topic: &str, partition: i32) {
        let mut topics = self.topics.lock().unwrap();
        let need = (partition + 1).max(1) as u32;
        let entry = topics.entry(topic.to_string()).or_insert(0);
        if *entry < need {
            *entry = need;
        }
        let count = *entry;
        drop(topics);
        let _ = self.router.create_topic(topic, count);
    }

    fn ensure_log(&self, topic: &str, partition: i32) -> Option<Arc<synapse_core::Log>> {
        let name = log_name(topic, partition);
        if self.core.get_log(TENANT, &name).unwrap().is_none() {
            if let Err(_) = self.core.create_log(TENANT, &name) {
                return None;
            }
        }
        self.core.get_log(TENANT, &name).unwrap()
    }

    /// Dispatch a decoded request frame, returning the encoded response body
    /// (without the leading `correlation_id`, which the server prepends).
    pub fn handle(&self, frame: &Frame) -> Vec<u8> {
        match frame.api_key {
            ApiKey::ApiVersions => encode_api_versions(),
            ApiKey::Produce => self.do_produce(&frame.body),
            ApiKey::Fetch => self.do_fetch(&frame.body),
            ApiKey::ListOffsets => self.do_list_offsets(&frame.body),
            ApiKey::Metadata => self.do_metadata(&frame.body),
            ApiKey::FindCoordinator => self.do_find_coordinator(&frame.body),
            ApiKey::OffsetCommit => self.do_offset_commit(&frame.body),
            ApiKey::OffsetFetch => self.do_offset_fetch(&frame.body),
            ApiKey::JoinGroup => self.do_join_group(&frame.body),
            ApiKey::SyncGroup => self.do_sync_group(&frame.body),
            ApiKey::Heartbeat => self.do_heartbeat(&frame.body),
            ApiKey::LeaveGroup => self.do_leave_group(&frame.body),
            ApiKey::Unknown(_) => encode_error_only(ERR_UNKNOWN),
        }
    }

    fn do_produce(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_produce(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let mut out = Vec::with_capacity(req.topics.len());
        for (topic, parts) in &req.topics {
            let mut out_parts = Vec::with_capacity(parts.len());
            for (partition, data) in parts {
                self.ensure_topic(topic, *partition);
                match self.ensure_log(topic, *partition) {
                    Some(log) => match log.append(data) {
                        Ok(offset) => out_parts.push((*partition, ERR_NONE, offset as i64)),
                        Err(_) => out_parts.push((*partition, ERR_UNKNOWN, -1)),
                    },
                    None => out_parts.push((*partition, ERR_UNKNOWN_TOPIC_OR_PARTITION, -1)),
                }
            }
            out.push((topic.clone(), out_parts));
        }
        encode_produce(&out)
    }

    fn do_fetch(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_fetch(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let mut out = Vec::with_capacity(req.topics.len());
        for (topic, parts) in &req.topics {
            let mut out_parts = Vec::with_capacity(parts.len());
            for (partition, fetch_offset, max_bytes) in parts {
                match self.core.get_log(TENANT, &log_name(topic, *partition)).unwrap() {
                    Some(log) => {
                        let hw = log.len();
                        let from = (*fetch_offset).max(0) as u64;
                        let records = log.read(from, 1024).unwrap_or_default();
                        let mut data = Vec::new();
                        for r in records {
                            if !data.is_empty()
                                && data.len() + r.payload.len() > *max_bytes as usize
                            {
                                break;
                            }
                            data.extend_from_slice(&r.payload);
                        }
                        out_parts.push((*partition, ERR_NONE, hw as i64, data));
                    }
                    None => out_parts.push((
                        *partition,
                        ERR_UNKNOWN_TOPIC_OR_PARTITION,
                        0,
                        Vec::new(),
                    )),
                }
            }
            out.push((topic.clone(), out_parts));
        }
        encode_fetch(&out)
    }

    fn do_list_offsets(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_list_offsets(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let mut out = Vec::with_capacity(req.topics.len());
        for (topic, parts) in &req.topics {
            let mut out_parts = Vec::with_capacity(parts.len());
            for (partition, timestamp) in parts {
                match self.core.get_log(TENANT, &log_name(topic, *partition)).unwrap() {
                    Some(log) => {
                        let offset = if *timestamp == -2 {
                            0
                        } else {
                            log.len() as i64
                        };
                        out_parts.push((*partition, ERR_NONE, offset));
                    }
                    None => out_parts.push((*partition, ERR_UNKNOWN_TOPIC_OR_PARTITION, 0)),
                }
            }
            out.push((topic.clone(), out_parts));
        }
        encode_list_offsets(&out)
    }

    fn do_metadata(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_metadata(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let topics = self.topics.lock().unwrap();
        let selected: Vec<(String, u32)> = match &req.topics {
            Some(names) => names
                .iter()
                .filter_map(|n| topics.get(n).map(|c| (n.clone(), *c)))
                .collect(),
            None => topics.iter().map(|(n, c)| (n.clone(), *c)).collect(),
        };
        drop(topics);
        encode_metadata("synapse", DEFAULT_PORT, &selected)
    }

    fn do_find_coordinator(&self, body: &[u8]) -> Vec<u8> {
        match parse_find_coordinator(body) {
            Ok(_) => encode_find_coordinator("synapse", DEFAULT_PORT),
            Err(_) => encode_error_only(ERR_COORDINATOR_NOT_AVAILABLE),
        }
    }

    fn do_offset_commit(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_offset_commit(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        for (topic, parts) in &req.topics {
            for (partition, offset, _) in parts {
                self.router.commit(&req.group_id, topic, *partition as u32, *offset as u64);
            }
        }
        let out: Vec<(String, Vec<(i32, i16)>)> = req
            .topics
            .iter()
            .map(|(t, parts)| {
                (t.clone(), parts.iter().map(|(p, _, _)| (*p, ERR_NONE)).collect())
            })
            .collect();
        encode_offset_commit(&out)
    }

    fn do_offset_fetch(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_offset_fetch(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let mut out = Vec::with_capacity(req.topics.len());
        for (topic, parts) in &req.topics {
            let mut out_parts = Vec::with_capacity(parts.len());
            for partition in parts {
                let offset = self.router.next_fetch(&req.group_id, topic, *partition as u32);
                out_parts.push((*partition, offset as i64, String::new(), ERR_NONE));
            }
            out.push((topic.clone(), out_parts));
        }
        encode_offset_fetch(&out)
    }

    fn do_join_group(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_join_group(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let member = match &req.member_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => format!("member-{}", self.next_member.fetch_add(1, Ordering::Relaxed)),
        };
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry(req.group_id.clone()).or_default();
        g.generation += 1;
        if !g.members.iter().any(|m| m == &member) {
            g.members.push(member.clone());
        }
        g.leader = Some(member.clone());
        let protocol = req
            .protocols
            .first()
            .map(|(name, _)| name.as_str())
            .unwrap_or("");
        let members: Vec<(String, Vec<u8>)> =
            g.members.iter().map(|m| (m.clone(), Vec::new())).collect();
        encode_join_group(g.generation, protocol, &member, &member, &members)
    }

    fn do_sync_group(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_sync_group(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let member = req.member_id.clone().unwrap_or_default();
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry(req.group_id.clone()).or_default();
        let mut assignment = Vec::new();
        for (id, data) in &req.assignment {
            if id == &member {
                assignment = data.clone();
            }
            g.assignments.insert(id.clone(), data.clone());
        }
        encode_sync_group(&assignment)
    }

    fn do_heartbeat(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_member_request(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let groups = self.groups.lock().unwrap();
        let ok = matches!(groups.get(&req.group_id), Some(g) if g.members.contains(&req.member_id.clone().unwrap_or_default()));
        drop(groups);
        if ok {
            encode_error_only(ERR_NONE)
        } else {
            encode_error_only(ERR_UNKNOWN)
        }
    }

    fn do_leave_group(&self, body: &[u8]) -> Vec<u8> {
        let req = match parse_member_request(body) {
            Ok(r) => r,
            Err(_) => return encode_error_only(ERR_UNKNOWN),
        };
        let member = req.member_id.clone().unwrap_or_default();
        let mut groups = self.groups.lock().unwrap();
        if let Some(g) = groups.get_mut(&req.group_id) {
            g.members.retain(|m| m != &member);
            if g.members.is_empty() {
                groups.remove(&req.group_id);
            }
        }
        encode_error_only(ERR_NONE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> Broker {
        Broker::new(Arc::new(SynapseCore::new()))
    }

    fn frame(api: ApiKey, body: Vec<u8>) -> Frame {
        Frame {
            api_key: api,
            api_version: 0,
            correlation_id: 1,
            client_id: Some("c".into()),
            body,
        }
    }

    #[test]
    fn produce_then_fetch() {
        let b = broker();
        let mut body = Vec::new();
        let mut w = crate::codec::Writer::new();
        w.i16(1);
        w.i32(1000);
        w.array_len(1);
        w.str_opt(Some("t"));
        w.array_len(1);
        w.i32(0);
        w.bytes_opt(Some(b"hello"));
        body.extend_from_slice(&w.buf);

        let resp = b.handle(&frame(ApiKey::Produce, body));
        // decode produces: 1 topic, 1 partition, error none, offset 0
        assert!(resp.len() > 0);

        let mut fb = Vec::new();
        let mut fw = crate::codec::Writer::new();
        fw.i32(-1); // replica_id
        fw.i32(0); // max_wait
        fw.i32(1); // min_bytes
        fw.array_len(1);
        fw.str_opt(Some("t"));
        fw.array_len(1);
        fw.i32(0); // partition
        fw.i64(0); // fetch_offset
        fw.i32(1024); // max_bytes
        fb.extend_from_slice(&fw.buf);
        let fresp = b.handle(&frame(ApiKey::Fetch, fb));
        // Fetch response: topic, partition, error, hw, record_set containing "hello"
        assert!(fresp.windows(5).any(|w| w == b"hello"));
    }

    #[test]
    fn offset_commit_and_fetch_roundtrip() {
        let b = broker();
        let mut body = Vec::new();
        let mut w = crate::codec::Writer::new();
        w.str_opt(Some("g1"));
        w.array_len(1);
        w.str_opt(Some("t"));
        w.array_len(1);
        w.i32(0);
        w.i64(42);
        w.str_opt(Some(""));
        body.extend_from_slice(&w.buf);
        b.handle(&frame(ApiKey::OffsetCommit, body));

        let mut fb = Vec::new();
        let mut fw = crate::codec::Writer::new();
        fw.str_opt(Some("g1"));
        fw.array_len(1);
        fw.str_opt(Some("t"));
        fw.array_len(1);
        fw.i32(0);
        fb.extend_from_slice(&fw.buf);
        let resp = b.handle(&frame(ApiKey::OffsetFetch, fb));
        // offset 42 + empty metadata + error none
        let off_bytes = 42i64.to_be_bytes();
        assert!(resp.windows(8).any(|w| w == &off_bytes));
    }
}
