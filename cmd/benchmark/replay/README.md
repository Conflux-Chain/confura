# Multi-address eth_getLogs replay

Run baseline and optimized instances against the same stable MySQL dataset. Enable
`ethstore.mysql.getLogsRangeScan.enabled` only on the optimized instance. Defaults
are `minAddresses: 32`, `maxBlocks: 0`; the candidate cap is fixed at 10000.
For tests above 32 addresses, configure `requestControl.logFilter.maxAddressCount`
on both instances accordingly. No schema migration is required.

Save a representative JSON RPC filter (not the entire JSON-RPC envelope) with
explicit finalized `fromBlock`, `toBlock`, `address` array and optional `topics`.
Use real contract addresses; the script never fabricates or repeats addresses.

```sh
python3 cmd/benchmark/replay/eth_getlogs.py \
  --baseline http://127.0.0.1:8545 --optimized http://127.0.0.1:8546 \
  --filter /tmp/swappi-filter.json --counts 1,10,32,50,100,200,500,921 \
  --iterations 100 --concurrency 1
```

Use a file with at least 2000 addresses for the default full matrix. Repeat with
single-block, short and wide ranges, sparse/dense blocks and different topics.
Run warm-up separately, then concurrency 1, 4 and 8 with ingest active. The script
prints JSON lines with p50/p95/p99, error rate and exact response comparison; any
mismatch or RPC/transport error yields a nonzero exit status. Stable finalized
ranges are required for meaningful equality checks. It does not change server
configuration or database contents.

Collect server-side measurements alongside replay: SQL count and cumulative SQL
time (GORM tracing), pool WaitCount/WaitDuration deltas, process CPU/RSS/heap,
MySQL QPS/CPU/IO and EXPLAIN for the estimator and range reader. Pool statistics
are aggregate and should not be interpreted as per-request waits under load.
Record the data shape and configuration with results. Avoid verbose SQL logging
in the timed run unless enabled identically on both instances.

New metrics use the prefix `infura/store/ethGetLogs/rangeScan/`: outcome counters
(`addresses`, `blocks`, `coverage`, `estimate`, `actualCount`, `hit`, `error`),
`estimate` timer and `candidates` histogram for accepted estimates. No filter or
address values are metric labels. A hit avoids address-ID lookup and migration
state checks. For P universal partitions the path uses 1 router + P estimator +
P reader SQL statements, including when no candidate logs match.

Acceptance: identical results, at least 50% lower p95 on the representative
921-address/one-block case when eligible, SQL counts independent of address
count on that path, and no error-rate deterioration under mixed load. Roll out
with the switch disabled, enable on the Swappi instance, monitor latency,
indexing lag, fallback rate and memory, and disable/restart to roll back.
Candidate logs have a row cap only; final response byte checks remain in the RPC
handler. Reorg consistency is unchanged; no cross-query snapshot is introduced.
