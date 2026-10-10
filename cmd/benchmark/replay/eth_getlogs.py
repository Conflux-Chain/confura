#!/usr/bin/env python3
"""Compare two read-only eth_getLogs endpoints against the same stable dataset."""
import argparse
import concurrent.futures
import json
import math
import time
import urllib.request


def request(url, query, timeout):
    body = json.dumps(dict(jsonrpc="2.0", id=1, method="eth_getLogs", params=[query])).encode()
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, body, {"Content-Type": "application/json"}), timeout=timeout) as response:
            payload = json.load(response)
        value = {k: payload[k] for k in ("result", "error") if k in payload}
        return (time.perf_counter() - started) * 1000, value, "error" in payload
    except Exception as exc:
        return (time.perf_counter() - started) * 1000, {"transport_error": type(exc).__name__}, True


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--optimized", required=True)
    parser.add_argument("--filter", required=True, help="JSON file containing an RPC filter with representative addresses")
    parser.add_argument("--counts", default="1,10,32,50,100,200,500,1000,2000")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if args.iterations < 1 or args.concurrency < 1:
        parser.error("iterations and concurrency must be positive")
    with open(args.filter) as source:
        query = json.load(source)
    addresses = query["address"]
    if not isinstance(addresses, list):
        parser.error("filter.address must be an array")
    counts = [int(value) for value in args.counts.split(",")]
    if any(count < 1 or count > len(addresses) for count in counts):
        parser.error("each count must be positive and within the supplied address list; use --counts for a 921-address replay")
    failed = False
    for count in counts:
        current = dict(query, address=addresses[:count])
        def pair(index):
            # Alternate order to reduce systematic cache-order bias.
            urls = [args.baseline, args.optimized]
            order = [0, 1] if index % 2 == 0 else [1, 0]
            results = {}
            for side in order:
                results[side] = request(urls[side], current, args.timeout)
            return results[0], results[1]
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            samples = list(pool.map(pair, range(args.iterations)))
        mismatches = sum(left[1] != right[1] for left, right in samples)
        failed |= mismatches > 0 or any(left[2] or right[2] for left, right in samples)
        report = dict(addresses=count, concurrency=args.concurrency, mismatches=mismatches)
        for side, name in enumerate(("baseline", "optimized")):
            times = [sample[side][0] for sample in samples]
            report[name] = {"p50_ms": percentile(times, .50), "p95_ms": percentile(times, .95), "p99_ms": percentile(times, .99), "error_rate": sum(sample[side][2] for sample in samples) / len(samples)}
        print(json.dumps(report), flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
