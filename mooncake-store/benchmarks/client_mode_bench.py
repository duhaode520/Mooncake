#!/usr/bin/env python3
"""
Benchmark comparing Mooncake Store embedded mode vs standalone mode.

Embedded mode: RealClient runs in-process (store.setup())
Standalone mode: DummyClient proxies to external RealClient (store.setup_dummy())

Measures batch_put_from and batch_get_into latency/throughput across multiple
value sizes, with warmup, multi-threading, and full output (terminal/JSON/CSV).
"""

import argparse
import copy
import csv
import json
import logging
import math
import os
import statistics
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import ctypes
import numpy as np

from mooncake.store import MooncakeDistributedStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("client_mode_bench")


# ---------------------------------------------------------------------------
# Performance tracking
# ---------------------------------------------------------------------------

class PerformanceTracker:
    """Collects per-operation latency samples and computes statistics."""

    def __init__(self):
        self.latencies: List[float] = []  # seconds
        self.sizes: List[int] = []
        self.errors: Dict[int, int] = defaultdict(int)
        self.wall_start: float = sys.float_info.max
        self.wall_end: float = 0.0
        self.total_ops: int = 0
        self.failed_ops: int = 0
        self.bytes_transferred: int = 0

    def record(self, latency_s: float, data_bytes: int):
        self.latencies.append(latency_s)
        self.sizes.append(data_bytes)

    def record_error(self, code: int):
        self.errors[code] += 1
        self.failed_ops += 1

    def start(self):
        self.wall_start = time.perf_counter()

    def stop(self):
        self.wall_end = time.perf_counter()

    @property
    def wall_time(self) -> float:
        return max(self.wall_end - self.wall_start, 0.0)

    def merge(self, other: "PerformanceTracker"):
        self.latencies.extend(other.latencies)
        self.sizes.extend(other.sizes)
        self.total_ops += other.total_ops
        self.failed_ops += other.failed_ops
        self.bytes_transferred += other.bytes_transferred
        self.wall_start = min(self.wall_start, other.wall_start)
        self.wall_end = max(self.wall_end, other.wall_end)
        for c, n in other.errors.items():
            self.errors[c] += n

    def stats(self) -> Dict[str, Any]:
        if not self.latencies:
            return {"error": "no data"}
        ms = [l * 1000 for l in self.latencies]
        total_bytes = sum(self.sizes)
        op_time = sum(self.latencies)
        n = len(ms)

        def pct(p):
            if n < 2:
                return max(ms)
            idx = int(math.ceil(n * p)) - 1
            return sorted(ms)[min(idx, n - 1)]

        ops_s = n / op_time if op_time > 0 else 0
        wall_ops_s = n / self.wall_time if self.wall_time > 0 else 0
        mbps = total_bytes / op_time / (1024 * 1024) if op_time > 0 else 0
        wall_mbps = total_bytes / self.wall_time / (1024 * 1024) if self.wall_time > 0 else 0

        return {
            "total_ops": n,
            "succeeded": n - self.failed_ops,
            "failed": self.failed_ops,
            "total_bytes": total_bytes,
            "op_time_s": op_time,
            "wall_time_s": self.wall_time,
            "mean_ms": statistics.mean(ms),
            "min_ms": min(ms),
            "max_ms": max(ms),
            "p50_ms": pct(0.50),
            "p90_ms": pct(0.90),
            "p99_ms": pct(0.99),
            "p999_ms": pct(0.999),
            "ops_per_sec": ops_s,
            "wall_ops_per_sec": wall_ops_s,
            "throughput_mbps": mbps,
            "wall_throughput_mbps": wall_mbps,
            "errors": dict(self.errors),
        }


# ---------------------------------------------------------------------------
# Benchmark core
# ---------------------------------------------------------------------------

@dataclass
class BenchConfig:
    mode: str = "embedded"
    protocol: str = "tcp"
    device_name: str = ""
    local_hostname: str = "localhost"
    metadata_server: str = "http://127.0.0.1:8080/metadata"
    master_server: str = "localhost:50051"
    real_client_address: str = "127.0.0.1:50052"
    global_segment_size: int = 4 * 1024 * 1024 * 1024  # 4GB
    local_buffer_size: int = 512 * 1024 * 1024  # 512MB
    value_size: int = 1024
    batch_size: int = 1
    num_ops: int = 100
    warmup_ops: int = 10
    num_threads: int = 1
    thread_id: int = 0
    key_prefix: str = ""


class BenchInstance:
    """Runs put/get benchmark for a single value size configuration."""

    def __init__(self, cfg: BenchConfig):
        self.cfg = cfg
        self.store = MooncakeDistributedStore()
        self.put_tracker = PerformanceTracker()
        self.get_tracker = PerformanceTracker()
        self.buffer_array: Optional[np.ndarray] = None
        self.buffer_ptr: int = 0
        self.shm_ptr: int = 0

    # -- Setup / teardown -----------------------------------------------------

    def setup(self):
        c = self.cfg
        if c.mode == "embedded":
            rc = self.store.setup(
                c.local_hostname,
                c.metadata_server,
                c.global_segment_size,
                c.local_buffer_size,
                c.protocol,
                c.device_name,
                c.master_server,
            )
            if rc:
                raise RuntimeError(f"store.setup failed: {rc}")
            buf_size = c.batch_size * c.value_size
            self.buffer_array = np.zeros(buf_size, dtype=np.uint8)
            self.buffer_ptr = self.buffer_array.ctypes.data
            rc = self.store.register_buffer(self.buffer_ptr, buf_size)
            if rc:
                raise RuntimeError(f"register_buffer failed: {rc}")
        elif c.mode == "standalone":
            mem_pool_size = c.global_segment_size
            rc = self.store.setup_dummy(mem_pool_size, c.local_buffer_size, c.real_client_address)
            if rc:
                raise RuntimeError(f"store.setup_dummy failed: {rc}")
            buf_size = c.batch_size * c.value_size
            self.shm_ptr = self.store.alloc_from_mem_pool(buf_size)
            if not self.shm_ptr:
                raise RuntimeError("alloc_from_mem_pool returned null")
            self.buffer_ptr = self.shm_ptr
            rc = self.store.register_buffer(self.buffer_ptr, buf_size)
            if rc:
                raise RuntimeError(f"register_buffer failed (standalone): {rc}")
        else:
            raise ValueError(f"unknown mode: {c.mode}")
        logger.info(
            f"[thread-{c.thread_id}] setup complete  mode={c.mode}  "
            f"value_size={c.value_size}  batch_size={c.batch_size}"
        )

    def teardown(self):
        try:
            if self.buffer_ptr:
                self.store.unregister_buffer(self.buffer_ptr)
            self.store.close()
        except Exception as e:
            logger.warning(f"teardown error: {e}")

    # -- Helpers ---------------------------------------------------------------

    def _make_keys(self, base_idx: int) -> List[str]:
        return [
            f"{self.cfg.key_prefix}k_{self.cfg.thread_id}_{base_idx + i}"
            for i in range(self.cfg.batch_size)
        ]

    def _put_batch(self, keys: List[str]) -> List[int]:
        ptrs = []
        sizes = []
        for i in range(len(keys)):
            ptrs.append(self.buffer_ptr + i * self.cfg.value_size)
            sizes.append(self.cfg.value_size)
        return self.store.batch_put_from(keys, ptrs, sizes)

    def _get_batch(self, keys: List[str]) -> List[int]:
        ptrs = []
        sizes = []
        for i in range(len(keys)):
            ptrs.append(self.buffer_ptr + i * self.cfg.value_size)
            sizes.append(self.cfg.value_size)
        return self.store.batch_get_into(keys, ptrs, sizes)

    # -- Benchmark phases ------------------------------------------------------

    def warmup(self):
        logger.info(f"[thread-{self.cfg.thread_id}] warmup ({self.cfg.warmup_ops} ops)")
        for i in range(self.cfg.warmup_ops):
            keys = self._make_keys(1_000_000 + i * self.cfg.batch_size)
            self._put_batch(keys)
            self._get_batch(keys)

    def run_put(self):
        tracker = self.put_tracker
        tracker.start()
        done = 0
        while done < self.cfg.num_ops:
            keys = self._make_keys(done * self.cfg.batch_size)
            t0 = time.perf_counter()
            codes = self._put_batch(keys)
            t1 = time.perf_counter()
            ok = sum(1 for c in codes if c == 0)
            fail = len(codes) - ok
            tracker.total_ops += len(codes)
            if ok > 0:
                tracker.record(t1 - t0, ok * self.cfg.value_size)
                tracker.bytes_transferred += ok * self.cfg.value_size
            for c in codes:
                if c != 0:
                    tracker.record_error(c)
            done += 1
        tracker.stop()

    def run_get(self):
        tracker = self.get_tracker
        tracker.start()
        done = 0
        while done < self.cfg.num_ops:
            keys = self._make_keys(done * self.cfg.batch_size)
            t0 = time.perf_counter()
            codes = self._get_batch(keys)
            t1 = time.perf_counter()
            ok = sum(1 for c in codes if c > 0)
            fail = len(codes) - ok
            tracker.total_ops += len(codes)
            if ok > 0:
                tracker.record(t1 - t0, ok * self.cfg.value_size)
                tracker.bytes_transferred += ok * self.cfg.value_size
            for c in codes:
                if c < 0:
                    tracker.record_error(c)
            done += 1
        tracker.stop()


# ---------------------------------------------------------------------------
# Multi-thread orchestration
# ---------------------------------------------------------------------------

def run_bench_for_value_size(base_cfg: BenchConfig) -> Dict[str, Any]:
    """Run benchmark for one value size, returns combined stats."""
    if base_cfg.num_threads <= 1:
        inst = BenchInstance(base_cfg)
        inst.setup()
        inst.warmup()
        inst.run_put()
        inst.run_get()
        inst.teardown()
        return {
            "put": inst.put_tracker.stats(),
            "get": inst.get_tracker.stats(),
        }

    # Multi-threaded
    import queue

    results_q: queue.Queue = queue.Queue()
    barrier = threading.Barrier(base_cfg.num_threads)

    def worker(tid):
        cfg = copy.copy(base_cfg)
        cfg.thread_id = tid
        inst = BenchInstance(cfg)
        try:
            inst.setup()
            inst.warmup()
            barrier.wait()
            inst.run_put()
            barrier.wait()
            inst.run_get()
            inst.teardown()
            results_q.put((tid, inst.put_tracker, inst.get_tracker))
        except Exception as e:
            logger.error(f"worker-{tid} failed: {e}")
            results_q.put((tid, PerformanceTracker(), PerformanceTracker()))

    threads = []
    for i in range(base_cfg.num_threads):
        t = threading.Thread(target=worker, args=(i,), name=f"bench-{i}")
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    combined_put = PerformanceTracker()
    combined_get = PerformanceTracker()
    while not results_q.empty():
        _, pt, gt = results_q.get()
        combined_put.merge(pt)
        combined_get.merge(gt)

    return {
        "put": combined_put.stats(),
        "get": combined_get.stats(),
    }


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def format_bytes(size: int) -> str:
    if size == 0:
        return "0B"
    names = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size, 1024)))
    return f"{round(size / (1024 ** i), 2)} {names[i]}"


def print_summary_table(all_results: List[Dict[str, Any]]):
    """Print a concise terminal table."""
    header = (
        f"{'ValueSize':>10} {'Op':>4} {'Ops':>6} {'Mean(ms)':>10} "
        f"{'P50(ms)':>10} {'P90(ms)':>10} {'P99(ms)':>10} "
        f"{'Ops/s':>10} {'MB/s':>10} {'Fail':>6}"
    )
    logger.info("=" * len(header))
    logger.info(header)
    logger.info("-" * len(header))
    for r in all_results:
        vs = format_bytes(r["value_size"])
        for op in ("put", "get"):
            s = r[op]
            if "error" in s:
                logger.info(f"{vs:>10} {op:>4}  NO DATA")
                continue
            logger.info(
                f"{vs:>10} {op:>4} {s['total_ops']:>6} {s['mean_ms']:>10.2f} "
                f"{s['p50_ms']:>10.2f} {s['p90_ms']:>10.2f} {s['p99_ms']:>10.2f} "
                f"{s['wall_ops_per_sec']:>10.1f} {s['wall_throughput_mbps']:>10.1f} "
                f"{s['failed']:>6}"
            )
    logger.info("=" * len(header))


def save_json(results: List[Dict[str, Any]], path: str):
    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"JSON saved to {path}")


def save_csv(results: List[Dict[str, Any]], path: str):
    fields = [
        "value_size", "operation", "total_ops", "succeeded", "failed",
        "mean_ms", "min_ms", "max_ms", "p50_ms", "p90_ms", "p99_ms", "p999_ms",
        "ops_per_sec", "wall_ops_per_sec", "throughput_mbps", "wall_throughput_mbps",
        "wall_time_s", "total_bytes",
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            for op in ("put", "get"):
                s = r[op]
                if "error" in s:
                    continue
                row = {"value_size": r["value_size"], "operation": op}
                for k in fields[2:]:
                    row[k] = s.get(k, "")
                w.writerow(row)
    logger.info(f"CSV saved to {path}")


# ---------------------------------------------------------------------------
# Compare mode
# ---------------------------------------------------------------------------

def compare_results(dir_a: str, dir_b: str):
    """Load JSON results from two directories and print comparison."""
    def load(d):
        p = os.path.join(d, "results.json")
        with open(p) as f:
            return json.load(f)

    a_data = load(dir_a)
    b_data = load(dir_b)

    a_map = {r["value_size"]: r for r in a_data}
    b_map = {r["value_size"]: r for r in b_data}

    header = (
        f"{'ValueSize':>10} {'Op':>4} "
        f"{'A Mean(ms)':>12} {'B Mean(ms)':>12} {'Diff%':>8} "
        f"{'A P99(ms)':>12} {'B P99(ms)':>12} {'Diff%':>8} "
        f"{'A MB/s':>10} {'B MB/s':>10} {'Diff%':>8}"
    )
    print("=" * len(header))
    print(f"  A = {dir_a}")
    print(f"  B = {dir_b}")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for vs in sorted(set(list(a_map.keys()) + list(b_map.keys()))):
        ar = a_map.get(vs, {})
        br = b_map.get(vs, {})
        for op in ("put", "get"):
            a_s = ar.get(op, {})
            b_s = br.get(op, {})
            if "error" in a_s or "error" in b_s:
                continue

            def diff_pct(va, vb):
                if va == 0:
                    return "N/A"
                return f"{((vb - va) / va) * 100:+.1f}%"

            print(
                f"{format_bytes(vs):>10} {op:>4} "
                f"{a_s.get('mean_ms', 0):>12.2f} {b_s.get('mean_ms', 0):>12.2f} "
                f"{diff_pct(a_s.get('mean_ms', 0), b_s.get('mean_ms', 0)):>8} "
                f"{a_s.get('p99_ms', 0):>12.2f} {b_s.get('p99_ms', 0):>12.2f} "
                f"{diff_pct(a_s.get('p99_ms', 0), b_s.get('p99_ms', 0)):>8} "
                f"{a_s.get('wall_throughput_mbps', 0):>10.1f} "
                f"{b_s.get('wall_throughput_mbps', 0):>10.1f} "
                f"{diff_pct(a_s.get('wall_throughput_mbps', 0), b_s.get('wall_throughput_mbps', 0)):>8}"
            )
    print("=" * len(header))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Mooncake Store Embedded vs Standalone Benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--mode", choices=["embedded", "standalone"], default="embedded")
    p.add_argument("--protocol", default="tcp")
    p.add_argument("--device-name", default="")
    p.add_argument("--local-hostname", default="localhost")
    p.add_argument(
        "--metadata-server", default="http://127.0.0.1:8080/metadata"
    )
    p.add_argument("--master-server", default="localhost:50051")
    p.add_argument("--real-client-address", default="127.0.0.1:50052")
    p.add_argument(
        "--global-segment-size",
        type=int,
        default=4 * 1024,
        help="Global segment size in MB",
    )
    p.add_argument(
        "--local-buffer-size", type=int, default=512, help="Local buffer size in MB"
    )
    p.add_argument(
        "--value-sizes",
        default="1024,65536,1048576,4194304,16777216",
        help="Comma-separated value sizes in bytes",
    )
    p.add_argument("--batch-size", type=int, default=1)
    p.add_argument("--num-ops", type=int, default=100, help="Operations per value-size tier")
    p.add_argument("--warmup-ops", type=int, default=10)
    p.add_argument("--num-threads", type=int, default=1)
    p.add_argument("--output-dir", default="")
    p.add_argument(
        "--compare",
        nargs=2,
        metavar=("DIR_A", "DIR_B"),
        help="Compare two result directories instead of running benchmark",
    )
    return p.parse_args()


def main():
    args = parse_args()

    if args.compare:
        compare_results(args.compare[0], args.compare[1])
        return

    value_sizes = [int(x) for x in args.value_sizes.split(",")]

    logger.info("=== Mooncake Client Mode Benchmark ===")
    logger.info(f"Mode:        {args.mode}")
    logger.info(f"Protocol:    {args.protocol}")
    logger.info(f"Value sizes: {[format_bytes(v) for v in value_sizes]}")
    logger.info(f"Batch size:  {args.batch_size}")
    logger.info(f"Ops/tier:    {args.num_ops}")
    logger.info(f"Warmup:      {args.warmup_ops}")
    logger.info(f"Threads:     {args.num_threads}")

    all_results: List[Dict[str, Any]] = []

    for vs in value_sizes:
        logger.info(f"\n--- value_size = {format_bytes(vs)} ---")
        cfg = BenchConfig(
            mode=args.mode,
            protocol=args.protocol,
            device_name=args.device_name,
            local_hostname=args.local_hostname,
            metadata_server=args.metadata_server,
            master_server=args.master_server,
            real_client_address=args.real_client_address,
            global_segment_size=args.global_segment_size * 1024 * 1024,
            local_buffer_size=args.local_buffer_size * 1024 * 1024,
            value_size=vs,
            batch_size=args.batch_size,
            num_ops=args.num_ops,
            warmup_ops=args.warmup_ops,
            num_threads=args.num_threads,
            key_prefix=f"bench_{vs}_",
        )
        result = run_bench_for_value_size(cfg)
        result["value_size"] = vs
        result["mode"] = args.mode
        result["protocol"] = args.protocol
        result["batch_size"] = args.batch_size
        result["num_threads"] = args.num_threads
        all_results.append(result)

    print_summary_table(all_results)

    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
        save_json(all_results, os.path.join(args.output_dir, "results.json"))
        save_csv(all_results, os.path.join(args.output_dir, "results.csv"))


if __name__ == "__main__":
    main()
