use labrpc::*;
use std::{thread, time};

use crate::msg::*;
use crate::server::{Write};
use crate::service::{TSOClient, TransactionClient};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    writes: Vec<Write>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            writes: Vec::new()
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        let request = TimestampRequest{};

        let mut bf_time_ms = BACKOFF_TIME_MS;

        for attempt in 0..RETRY_TIMES {
            let rpc_future = self.tso_client.get_timestamp(&request);

            match futures::executor::block_on(rpc_future) {
                Ok(t) => return Ok(t.timestamp),
                Err(e) => {
                    eprintln!("RPC failure getting timestamp: {:?}", e);
                    if attempt < RETRY_TIMES - 1 {
                        thread::sleep(time::Duration::from_millis(bf_time_ms));
                        bf_time_ms *= 2;
                    }
                }
            }
        }

        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        if let Ok(ts) = self.get_timestamp() {
            self.start_ts = ts;
            self.writes.clear();
        }
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        let request = GetRequest{
            key,
            ts_start: self.start_ts
        };

        let mut bf_time_ms = BACKOFF_TIME_MS;

        for attempt in 0..RETRY_TIMES {
            let rpc_future = self.txn_client.get(&request);

            match futures::executor::block_on(rpc_future) {
                Ok(r) => {
                    if r.is_found {
                        return Ok(r.value);
                    } else {
                        return Ok(Vec::new());
                    }
                },
                Err(e) => { eprintln!("RPC failure getting timestamp: {:?}", e); }
            }
            if attempt < RETRY_TIMES - 1 {
                thread::sleep(time::Duration::from_millis(bf_time_ms));
                bf_time_ms *= 2;
            }
        }

        Err(labrpc::Error::Other("Timeout".to_string()))
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push(Write(key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Fast path for empty transactions
        if self.writes.is_empty() {
            return Ok(true);
        }

        // Extract the Primary Key safely
        let primary = self.writes.first().unwrap().0.clone();

        // Phase 1: Prewrite all keys
        for write_item in &self.writes {
            let key = &write_item.0;
            let value = &write_item.1;

            let prewrite_req = PrewriteRequest {
                key: key.clone(),
                value: value.clone(),
                primary: primary.clone(),
                ts_start: self.start_ts,
            };

            let mut bf_time_ms = BACKOFF_TIME_MS;
            let mut prewrite_success = false;

            let mut err: Option<Error> = None;

            for attempt in 0..RETRY_TIMES {
                let rpc_future = self.txn_client.prewrite(&prewrite_req);

                match futures::executor::block_on(rpc_future) {
                    Ok(r) => {
                        if r.success {
                            prewrite_success = true;
                            break;
                        } else {
                            // If any prewrite fails, the transaction is aborted
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        eprintln!("RPC failure during prewrite: {:?}", e);
                        err = Some(e);
                    }
                }
                
                if attempt < RETRY_TIMES - 1 {
                    thread::sleep(time::Duration::from_millis(bf_time_ms));
                    bf_time_ms *= 2;
                }
            }

            if !prewrite_success {
                return Err(err.unwrap());
            }
        }

        // Fetch Commit Timestamp
        let commit_ts = match self.get_timestamp() {
            Ok(ts) => ts,
            Err(_) => return Ok(false),
        };

        // Phase 2: Commit the Primary Key FIRST
        let commit_req = CommitRequest {
            key: primary.clone(),
            ts_start: self.start_ts,
            ts_commit: commit_ts,
            is_primary: true,
        };

        let mut bf_time_ms = BACKOFF_TIME_MS;
        let mut primary_committed = false;
        let mut err: Option<Error> = None;

        for attempt in 0..RETRY_TIMES {
            let rpc_future = self.txn_client.commit(&commit_req);

            match futures::executor::block_on(rpc_future) {
                Ok(r) => {
                    if r.success {
                        primary_committed = true;
                        break;
                    } else {
                        // If primary commit fails, the transaction fails
                        return Ok(false);
                    }
                }
                Err(e) => {
                    eprintln!("RPC failure committing primary: {:?}", e);
                    err = Some(e);
                }
            }
            if attempt < RETRY_TIMES - 1 {
                thread::sleep(time::Duration::from_millis(bf_time_ms));
                bf_time_ms *= 2;
            }
        }

        if !primary_committed {
            if let Some(e) = err {
                // If the error guarantees the request never reached the server ("reqhook"), 
                // we know definitively that the transaction aborted.
                if let labrpc::Error::Other(ref msg) = e {
                    if msg == "reqhook" {
                        return Ok(false);
                    }
                }
                // If we aren't sure (e.g., dropped response "resphook" or timeout), bubble up the error.
                return Err(e);
            }
            return Ok(false);
        }

        // Phase 2: Commit the Secondary Keys
        for write_item in &self.writes {
            let key = &write_item.0;
            let value = &write_item.1;

            if key == &primary {
                continue;
            }

            let commit_req = CommitRequest {
                key: key.clone(),
                ts_start: self.start_ts,
                ts_commit: commit_ts,
                is_primary: false,
            };

            let mut bf_time_ms = BACKOFF_TIME_MS;
            
            for attempt in 0..RETRY_TIMES {
                let rpc_future = self.txn_client.commit(&commit_req);

                match futures::executor::block_on(rpc_future) {
                    Ok(r) => {
                        if r.success { break; }
                    }
                    Err(e) => {
                        eprintln!("RPC failure committing secondary: {:?}", e);
                    }
                }
                if attempt < RETRY_TIMES - 1 {
                    thread::sleep(time::Duration::from_millis(bf_time_ms));
                    bf_time_ms *= 2;
                }
            }
        }

        // Successfully completed 2PC
        Ok(true)
    }
}
