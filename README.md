# Percolator

An educational implementation of Google's Percolator-style distributed transactions in Rust.

Table of Contents
-----------------

- [Overview](#overview)
- [Differences from the Percolator paper](#differences-from-the-percolator-paper)
- [Testing](#testing)
- [References](#references)

Overview
--------

This repository provides an educational, simplified implementation of the
Percolator transaction model (two-phase commit with distributed locking and
timestamped commits). The goal is to make the paper's ideas concrete for
learning and experiments, not to be a production-ready distributed database.


Differences from the Percolator paper
-------------------------------------

This project intentionally deviates from the original paper to simplify the lab
and to accommodate a compact in-memory implementation. Key differences:

1. "Smart Server" vs "Smart Client"
   - Paper: the client library is the smart coordinator that polls, backoffs,
     and cleans up locks.
   - This repo: the server takes on collision resolution and the
     backoff/cleanup loop (`back_off_maybe_clean_up_lock`) inside the `get` RPC
handler. This keeps client code simpler but centralises complexity on the
server.

2. Concurrency granularity
   - Paper: leverages Bigtable single-row transactions for row-level atomicity
     and high throughput across keys.
   - This repo: simulates row-level atomicity with a global `Mutex` wrapping
     the `KvTable`. Each RPC briefly locks the entire table to guarantee
atomicity for checks and mutations. This reduces concurrency and is intended
only for a lab environment.


Testing
-------

Run `cargo test`.

The test workload focuses on verifying ACID transactional guarantees and fault tolerance under simulated concurrency and network anomalies.

```
$ cargo test
...
running 13 tests
test tests::test_predicate_many_preceders_read_predicates ... ok
test tests::test_lost_update ... ok
test tests::test_anti_dependency_cycles ... ok
test tests::test_predicate_many_preceders_write_predicates ... ok
test tests::test_read_skew_predicate_dependencies ... ok
test tests::test_read_skew_read_only ... ok
test tests::test_read_skew_write_predicate ... ok
test tests::test_write_skew ... ok
test tests::test_commit_primary_success_without_response ... ok
test tests::test_commit_primary_fail ... ok
test tests::test_get_timestamp_under_unreliable_network ... ok
test tests::test_commit_primary_success ... ok
test tests::test_commit_primary_drop_secondary_requests ... ok

test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;
finished in 0.83
```

References
----------

- [Large-scale Incremental Processing Using Distributed Transactions, 2010](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Peng.pdf)

