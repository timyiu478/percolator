# Percolator

A distributed transaction model developed by Google to support ACID-compliant transactions across large-scale, distributed storage 

## Implementation Deviations from the Percolator Paper (Figure 6)

This repository implements a distributed transaction engine inspired by Google's Percolator. While the core 2-Phase Commit (2PC) logic remains identical to Figure 6 of the original paper, there are three key architectural deviations designed to simplify the laboratory environment.

### 1. "Smart Server" vs. "Smart Client"

* **The Paper:** The client library acts as the "Smart Coordinator." It continuously queries Bigtable, looping to check locks, handling exponential backoffs, and executing roll-forwards/roll-backs locally before reading data. Bigtable acts as a "dumb" storage layer.
* **This Lab:** The architecture shifts weight to a "Smart Server." The collision resolution and back-off loop (`back_off_maybe_clean_up_lock`) are executed directly within the server's `get` RPC handler. This reduces network chatter and simplifies the client library.

### 2. Lock Erasure and Tombstones

* **The Paper:** Bigtable supports range tombstones. When the client commits, it calls `T.Erase(row, lock_col, commit_ts)`, instructing Bigtable to delete all lock variants up to the `commit_ts`.
* **This Lab:** The memory layer uses a standard Rust `BTreeMap<(Vec<u8>, u64), Value>`, requiring exact coordinate matches. Instead of a range tombstone, the server explicitly erases locks at their exact staging timestamp by passing `start_ts` to the `erase` method.

### 3. Concurrency Granularity

* **The Paper:** Server-side atomicity relies on Bigtable's single-row transactions (`StartRowTransaction()`), allowing high throughput across different keys.
* **This Lab:** Row-level atomicity is simulated using a global `Mutex` wrapping the `KvTable`. Each RPC (`prewrite`, `commit`, `get`) briefly locks the entire table to ensure atomic checks and mutations.
