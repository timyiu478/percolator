use std::ops::Bound;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    last_timestamp: AtomicU64,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        let timestamp = self.last_timestamp.fetch_add(1, Ordering::SeqCst);

        Ok(TimestampResponse{timestamp})
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }

        let start_ts = ts_start_inclusive.unwrap_or(0);
        let end_ts = ts_end_inclusive.unwrap_or(std::u64::MAX);

        let start_bound = Bound::Included(&(key.clone(), start_ts));
        let end_bound = Bound::Included(&(key, end_ts));

        map.range((start_bound, end_bound)).next_back()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let mut map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }

        map.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let mut map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }

        map.remove((key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    // ref: paper's figure 6 line 8 - 24
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {

        let ts_start = Some(req.ts_start);
        let key = req.key;

        loop {
            let data = self.data.lock().unwrap();

            // Check for locks that signal concurrent writes
            if let Some((lock_key, _)) = data.read(key.clone(), Column::Lock, None, ts_start) {
                let lock_ts = lock_key.1;
                let lock_row = lock_key.0.clone();

                drop(data);

                self.back_off_maybe_clean_up_lock(lock_ts, lock_row);
                continue;
            }

            if let Some((_, write_val)) = data.read(key.clone(), Column::Write, None, ts_start) {
                if let Value::Timestamp(ts_data) = write_val {
                    if let Some((k, data_val)) = data.read(key.clone(), Column::Data, Some(*ts_data), Some(*ts_data)) {
                        if let Value::Vector(raw_bytes) = data_val {
                            return Ok(GetResponse {
                                value: raw_bytes.clone(),
                                is_found: true,
                            });
                        }
                    }
                }
            }

            return Ok(GetResponse{
                is_found: false,
                ..Default::default()
            })
        }
    }

    // example prewrite RPC handler.
    // ref: paper's figure 6 line 26 - 40
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let mut data = self.data.lock().unwrap();

        // Abort on writes after our start timestamp
        if data.read(req.key.clone(), Column::Write, Some(req.ts_start), None).is_some() || data.read(key.clone(), Column::Lock, None, None).is_some() {
            return Ok(PrewriteResponse{
                success: false
            });
        }

        data.write(req.key.clone(), Column::Data, req.ts_start, Value::Vector(req.value));
        data.write(req.key, Column::Lock, req.ts_start, Value::Vector(req.primary));

        Ok(PrewriteResponse{
            success: true
        });
    }

    // example commit RPC handler.
    // ref: paper's figure 6 line 52 - 58
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut data = self.data.lock().unwrap();

        if data.read(req.key.clone(), Column::Lock, Some(req.ts_start), Some(req.ts_start)).is_none() {
            return Ok(CommitResponse{
                success: false
            });
        }

        data.write(req.key.clone(), Column::Write, req.ts_commit, Value::Timestamp(req.ts_start));

        // this maps directly to line 57 in the pseudo-code, adapted for our BTreeMap storage
        data.erase(req.key, Column::Lock, req.ts_start);

        Ok(CommitResponse{
            success: true
        })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        std::thread::sleep(std::time::Duration::from_nanos(TTL));

        let mut data = self.data.lock().unwrap();
        
        // fetch the lock payload to read the primary key pointer
        if let Some((_, Value::Vector(primary_key))) = data.read(key.clone(), Column::Lock, Some(start_ts), Some(start_ts)) {
            let primary_key = primary_key.clone();

            // Check if the Primary Key was committed
            let primary_write = data.read(primary_key.clone(), Column::Write, Some(start_ts), None);
            
            if let Some((write_key, Value::Timestamp(target_start_ts))) = primary_write {
                if *target_start_ts == start_ts {
                    // Primary committed. Roll forward
                    let commit_ts = write_key.1;
                    data.write(key.clone(), Column::Write, commit_ts, Value::Timestamp(start_ts));
                    data.erase(key, Column::Lock, start_ts);
                    return;
                }
            }

            // Rollback
            data.erase(key.clone(), Column::Lock, start_ts);
            data.erase(key, Column::Data, start_ts);
        }
    }
}
