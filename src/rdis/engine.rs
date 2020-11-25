use super::protocol::RESP;
use log::{debug, error, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, oneshot};
use RESP::*;

type RawValue = Vec<u8>;
use super::types::ResultT;
use std::time::{SystemTime, UNIX_EPOCH};

type Key = Arc<RawValue>;
// contains the common data structures
struct RedisData {
    single_map: HashMap<Key, Arc<RawValue>>,
    list_map: HashMap<Key, VecDeque<Arc<RawValue>>>,
    eviction: BTreeMap<u64, HashSet<Key>>,
    last_evicted_t: u64,
}

const DEFAULT_CAPACITY: usize = 4096;
const DEFAULT_LIST_CAPACITY: usize = 8;

impl RedisData {
    fn new() -> RedisData {
        RedisData {
            single_map: HashMap::with_capacity(DEFAULT_CAPACITY),
            list_map: HashMap::with_capacity(DEFAULT_CAPACITY),
            eviction: BTreeMap::new(),
            last_evicted_t: 0,
        }
    }

    fn evict_if_needed(&mut self, t: u64) {
        let to_remove: Vec<u64> = self
            .eviction
            .range(self.last_evicted_t..t)
            .map(|(k, _)| k.clone())
            .collect();
        self.last_evicted_t = t;

        for k in to_remove {
            if let Some(values) = self.eviction.remove(&k) {
                for v in values {
                    self.single_map.remove(&v);
                }
            }
        }
    }

    fn insert_eviction(&mut self, k: Key, t: u64) {
        let set = match self.eviction.get_mut(&t) {
            Some(l) => l,
            None => {
                let s = HashSet::new();
                self.eviction.insert(t, s);
                self.eviction.get_mut(&t).unwrap()
            }
        };
        set.insert(k);
    }

    fn set(&mut self, k: Arc<RawValue>, v: Arc<RawValue>, evict_at: Option<u64>) {
        self.single_map.insert(k.clone(), v);
        evict_at.map(|t| self.insert_eviction(k, t));
    }

    fn get(&mut self, k: &RawValue, t: u64) -> Option<Arc<RawValue>> {
        self.evict_if_needed(t);
        self.single_map.get(k).map(|el| el.clone())
    }

    fn incr(&mut self, k: &RawValue, t: u64) -> ResultT<Option<i64>> {
        self.evict_if_needed(t);
        match self.single_map.get(k) {
            None => Ok(None),
            Some(int_raw) => {
                let i_decimal: i64 = String::from_utf8(int_raw.clone().to_vec())?.parse()?;
                Ok(Some(i_decimal + 1))
            }
        }
    }

    fn l_push(&mut self, k: Arc<RawValue>, v: Arc<RawValue>, evict_at: Option<u64>) {
        evict_at.map(|t| self.insert_eviction(k.clone(), t));
        let deq = self
            .list_map
            .entry(k.clone())
            .or_insert(VecDeque::with_capacity(DEFAULT_LIST_CAPACITY));
        deq.push_front(v);
    }

    fn r_push(&mut self, k: Arc<RawValue>, v: Arc<RawValue>, evict_at: Option<u64>) {
        evict_at.map(|t| self.insert_eviction(k.clone(), t));
        let deq = self
            .list_map
            .entry(k.clone())
            .or_insert(VecDeque::with_capacity(DEFAULT_LIST_CAPACITY));
        deq.push_back(v);
    }

    fn l_pop(&mut self, k: &RawValue) -> Option<Arc<RawValue>> {
        self.list_map.get_mut(k).and_then(|list| list.pop_front())
    }

    fn r_pop(&mut self, k: &RawValue) -> Option<Arc<RawValue>> {
        self.list_map.get_mut(k).and_then(|list| list.pop_back())
    }
}

pub struct RedisEngine {
    data: RedisData,
    receiver: mpsc::Receiver<(RESP, oneshot::Sender<RESP>)>,
    runtime: Runtime,
}

impl RedisEngine {
    pub fn new(receiver: mpsc::Receiver<(RESP, oneshot::Sender<RESP>)>) -> RedisEngine {
        let data = RedisData::new();
        let runtime = Builder::new_current_thread().build().unwrap();
        RedisEngine {
            data,
            receiver,
            runtime,
        }
    }

    fn current_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn start_loop(&mut self) -> () {
        loop {
            match self.runtime.block_on(self.receiver.recv()) {
                Some((req, channel)) => {
                    let t = RedisEngine::current_time();
                    channel.send(self.handle_request(req, t)).unwrap()
                }
                None => {
                    // TODO stay alive
                    warn!("No senders, loop terminated");
                }
            }
        }
    }

    fn handle_request(&mut self, req: RESP, t: u64) -> RESP {
        match req {
            Array(commands) => match commands.as_slice() {
                [] => Error("todo".into(), "empty command".into()),
                [BulkString(single)] => match single.as_slice() {
                    b"PING" => SimpleString("PONG".into()),
                    b"COMMAND" => SimpleString("OK".into()),
                    _ => RedisEngine::error_resp(),
                },
                [BulkString(cmd), BulkString(k)] => match cmd.as_slice() {
                    b"GET" => self
                        .data
                        .get(k, t)
                        .map_or(RESP::Null, |v| BulkString(v.clone())),
                    b"INCR" => match self.data.incr(k, t) {
                        Ok(res) => res.map_or(RESP::Null, |i| SimpleString(i.to_string().into())),
                        Err(err) => Error("WRONG_TYPE".into(), err.to_string()),
                    },
                    b"LPOP" => self.data.l_pop(k).map_or(RESP::Null, |v| BulkString(v)),
                    b"RPOP" => self.data.r_pop(k).map_or(RESP::Null, |v| BulkString(v)),
                    _ => RedisEngine::error_resp(),
                },
                [BulkString(cmd), BulkString(k), BulkString(v)] => match cmd.as_slice() {
                    b"SET" => {
                        self.data.set(k.clone(), v.clone(), None);
                        RedisEngine::ok()
                    }
                    b"LPUSH" => {
                        self.data.l_push(k.clone(), v.clone(), None);
                        RedisEngine::ok()
                    }
                    b"RPUSH" => {
                        self.data.r_push(k.clone(), v.clone(), None);
                        RedisEngine::ok()
                    }
                    _ => RedisEngine::error_resp(),
                },
                _ => RedisEngine::error_resp(),
            },
            other => self.handle_request(Array(vec![other]), t),
        }
    }

    fn error_resp() -> RESP {
        Error("Error".into(), "too many arguments".into())
    }

    fn ok() -> RESP {
        SimpleString("OK".into())
    }
}
