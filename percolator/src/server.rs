use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use futures_timer::Delay;
use labrpc::Result as RpcResult;

use crate::{
    msg::*,
    service::{TsoService, TxnService},
};

#[derive(Clone, Default)]
pub struct TimestampOracle {
    timestamp: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl TsoService for TimestampOracle {
    async fn get_timestamp(&self, _: TimestampArgs) -> RpcResult<TimestampReply> {
        Ok(TimestampReply {
            timestamp: self.timestamp.fetch_add(1, Ordering::Relaxed),
        })
    }
}

#[derive(Clone, Default)]
pub struct MemoryStorage {
    store: Arc<Mutex<Store>>,
}

impl MemoryStorage {
    const CHECK_PERIOD_MILLIS: u64 = 50;
    const CHECK_PERIOD: Duration = Duration::from_millis(Self::CHECK_PERIOD_MILLIS);
}

#[async_trait::async_trait]
impl TxnService for MemoryStorage {
    async fn get(&self, args: GetArgs) -> RpcResult<GetReply> {
        let store = loop {
            {
                let mut store = self.store.lock().unwrap();
                let (start_ts, lock) = match store.lock.read(&args.key, ..=args.start_ts) {
                    Some((key, lock)) => (key.timestamp, lock),
                    None => break store,
                };

                let key = if let Lock::Secondary(primary_key) = lock {
                    let primary_key = primary_key.clone();
                    if store.lock.read(&primary_key, start_ts..=start_ts).is_none() {
                        _ = store.lock.erase(&Key::new(args.key.clone(), start_ts));
                        let commit_ts = match store.write.read(&primary_key, start_ts..) {
                            Some((key, _)) => key.timestamp,
                            None => break store,
                        };

                        store
                            .write
                            .write(Key::new(args.key.clone(), commit_ts), start_ts);

                        break store;
                    }

                    primary_key
                } else {
                    args.key.clone()
                };

                let key = Key::new(key, start_ts);
                if store.lock.expire(&key) {
                    store.data.erase(&key);
                    break store;
                }
            }

            Delay::new(Self::CHECK_PERIOD).await;
        };

        let start_ts = match store.write.read(&args.key, ..=args.start_ts) {
            Some((_, timestamp)) => *timestamp,
            None => return Ok(GetReply::new(Vec::new())),
        };

        let (_, value) = store.data.read(&args.key, start_ts..=start_ts).unwrap();
        Ok(GetReply::new(value.clone()))
    }

    async fn prewrite(&self, args: PrewriteArgs) -> RpcResult<PrewriteReply> {
        let mut store = self.store.lock().unwrap();
        if store.lock.read(&args.key, ..).is_some()
            || store.write.read(&args.key, args.start_ts..).is_some()
        {
            return Ok(PrewriteReply::new(false));
        }

        let lock = if args.key == args.primary_key {
            Lock::Primary(Instant::now())
        } else {
            Lock::Secondary(args.primary_key)
        };

        store
            .lock
            .write(Key::new(args.key.clone(), args.start_ts), lock);

        store
            .data
            .write(Key::new(args.key, args.start_ts), args.value);

        Ok(PrewriteReply::new(true))
    }

    async fn commit(&self, args: CommitArgs) -> RpcResult<CommitReply> {
        let mut store = self.store.lock().unwrap();
        let lock = store.lock.erase(&Key::new(args.key.clone(), args.start_ts));

        if args.is_primary && lock.is_none() {
            return Ok(CommitReply::new(false));
        }

        store
            .write
            .write(Key::new(args.key, args.commit_ts), args.start_ts);

        Ok(CommitReply::new(true))
    }
}

impl GetReply {
    fn new(value: Vec<u8>) -> Self {
        Self { value }
    }
}

impl PrewriteReply {
    fn new(success: bool) -> Self {
        Self { success }
    }
}

impl CommitReply {
    fn new(success: bool) -> Self {
        Self { success }
    }
}

#[derive(Default)]
struct Store {
    lock: Table<Lock>,

    write: Table<u64>,
    data: Table<Vec<u8>>,
}

struct Table<T> {
    table: BTreeMap<Key, T>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    key: Vec<u8>,

    timestamp: u64,
}

enum Lock {
    Primary(Instant),
    Secondary(Vec<u8>),
}

impl<T> Table<T> {
    fn read(&self, key: &[u8], timestamps: impl RangeBounds<u64>) -> Option<(&Key, &T)> {
        let start_ts = match timestamps.start_bound() {
            Bound::Included(timestamp) => *timestamp,
            Bound::Excluded(_) => unreachable!(),
            Bound::Unbounded => 0,
        };

        let end_ts = match timestamps.end_bound() {
            Bound::Included(timestamp) => *timestamp + 1,
            Bound::Excluded(timestamp) => *timestamp,
            Bound::Unbounded => u64::MAX,
        };

        self.table
            .range(Key::new(key.into(), start_ts)..Key::new(key.into(), end_ts))
            .last()
    }

    fn write(&mut self, key: Key, value: T) {
        self.table.insert(key, value);
    }

    fn erase(&mut self, key: &Key) -> Option<T> {
        self.table.remove(key)
    }
}

impl Table<Lock> {
    const LOCK_TTL: Duration = Duration::from_millis(MemoryStorage::CHECK_PERIOD_MILLIS << 1);

    fn expire(&mut self, key: &Key) -> bool {
        let lock_at = match self.table.get(key).unwrap() {
            Lock::Primary(instant) => instant,
            Lock::Secondary(_) => unreachable!(),
        };

        if lock_at.elapsed() < Self::LOCK_TTL {
            return false;
        }

        _ = self.erase(key);
        true
    }
}

impl<T> Default for Table<T> {
    fn default() -> Self {
        Self {
            table: BTreeMap::new(),
        }
    }
}

impl Key {
    fn new(key: Vec<u8>, timestamp: u64) -> Self {
        Self { key, timestamp }
    }
}
