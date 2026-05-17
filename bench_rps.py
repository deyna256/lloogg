#!/usr/bin/env python3
"""Benchmark lloogg server PUSH throughput and latency.

Usage:
  ./bench_rps.py
  ./bench_rps.py --host 10.0.0.1 --port 7379 --connections 8 --count 50000
"""

from __future__ import annotations

import argparse
import asyncio
import struct
import time
import os

MAGIC_WIRE: int = 0xAE01
OPCODE_PUSH: int = 0x01
STATUS_OK: int = 0x00

RECORD_SIZE: int = 17
HEADER_SIZE: int = 9
PUSH_FRAME_SIZE: int = HEADER_SIZE + RECORD_SIZE  # 26
RESPONSE_SIZE: int = 5

PROGRESS_INTERVAL: float = 0.2
WARN_DEADLINE_S: float = 10.0


def build_push_frame(uid: int, event_type: int, timestamp: int, url_hash: int) -> bytearray:
    buf = bytearray(PUSH_FRAME_SIZE)
    struct.pack_into("<H", buf, 0, MAGIC_WIRE)
    struct.pack_into("<B", buf, 2, OPCODE_PUSH)
    struct.pack_into("<I", buf, 3, uid)
    struct.pack_into("<H", buf, 7, RECORD_SIZE)
    struct.pack_into("<B", buf, 9, event_type)
    struct.pack_into("<Q", buf, 10, timestamp)
    struct.pack_into("<Q", buf, 18, url_hash)
    return buf


async def bench_worker(
    host: str,
    port: int,
    uid: int,
    count: int,
    results: list[float],
    start_barrier: asyncio.Event,
    warmup: int,
    ready_event: asyncio.Event | None = None,
) -> int:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    reader, writer = await asyncio.open_connection(host, port)
    frame = build_push_frame(uid, 1, int(time.time()), 0)

    for _ in range(warmup):
        writer.write(frame)
        await writer.drain()
        resp = await reader.readexactly(RESPONSE_SIZE)
        if resp[2] != STATUS_OK:
            return 1

    if ready_event is not None:
        ready_event.set()

    await start_barrier.wait()

    errors = 0
    for _ in range(count):
        t0 = time.perf_counter()
        writer.write(frame)
        await writer.drain()
        resp = await reader.readexactly(RESPONSE_SIZE)
        if resp[2] != STATUS_OK:
            errors += 1
        results.append(time.perf_counter() - t0)

    writer.close()
    return errors


async def run_benchmark(args: argparse.Namespace) -> None:
    per_worker = max(1, args.count // args.connections)
    total = per_worker * args.connections
    warmup_per_worker = args.warmup // args.connections if args.warmup else 0

    print(f"Benchmarking lloogg at {args.host}:{args.port}")
    print(f"  connections={args.connections}  total={total}  warmup={args.warmup}")

    all_latencies: list[float] = []
    start_barrier = asyncio.Event()
    ready_events = [asyncio.Event() for _ in range(args.connections)]
    progress = 0
    total_errors = 0
    lock = asyncio.Lock()

    def progress_callback(n: int) -> None:
        nonlocal progress
        progress += n

    async def worker(uid_offset: int, ready_event: asyncio.Event) -> int:
        latencies: list[float] = []
        errs = await bench_worker(
            args.host, args.port, args.uid + uid_offset,
            per_worker, latencies, start_barrier, warmup_per_worker, ready_event,
        )
        async with lock:
            all_latencies.extend(latencies)
            nonlocal total_errors
            total_errors += errs
            progress_callback(len(latencies))
        return errs

    print("Warming up...", end="", flush=True)
    tasks = [
        asyncio.create_task(worker(i, ready_events[i]))
        for i in range(args.connections)
    ]
    await asyncio.gather(*(ready_event.wait() for ready_event in ready_events))
    print(" done")

    print("Measuring...")
    progress = 0
    t0 = time.monotonic()
    start_barrier.set()
    prev_progress = 0
    prev_time = t0
    deadline = t0 + WARN_DEADLINE_S

    while True:
        await asyncio.sleep(PROGRESS_INTERVAL)
        now = time.monotonic()
        done = progress
        pct = min(done * 100 // total, 100)
        elapsed = now - t0
        current_rps = (done - prev_progress) / (now - prev_time) if (now - prev_time) > 0 else 0
        prev_progress, prev_time = done, now
        print(f"\r  {pct:3d}%  ({done}/{total})  {current_rps:.0f} req/s  {elapsed:.1f}s", end="", flush=True)


        if done >= total:
            break
        if all(t.done() for t in tasks):
            break
        if now > deadline:
            print(f"\n  [warn] benchmark still running after {WARN_DEADLINE_S:.0f}s")
            deadline = now + WARN_DEADLINE_S

    await asyncio.gather(*tasks)
    elapsed = time.monotonic() - t0
    print()

    if not all_latencies:
        print("No requests completed successfully")
        return

    all_latencies.sort()
    successful = len(all_latencies)
    rps = successful / elapsed

    print(f"── Results ─────────────────────────────")
    print(f"  Successful  : {successful}")
    print(f"  Errors      : {total_errors}")
    print(f"  Wall time   : {elapsed:.2f}s")
    print(f"  Throughput  : {rps:.0f} req/s")
    print()
    print(f"── Latency ─────────────────────────────")
    print(f"  min   : {fmt_latency(all_latencies[0])}")
    print(f"  p50   : {fmt_latency(percentile(all_latencies, 50))}")
    print(f"  p90   : {fmt_latency(percentile(all_latencies, 90))}")
    print(f"  p95   : {fmt_latency(percentile(all_latencies, 95))}")
    print(f"  p99   : {fmt_latency(percentile(all_latencies, 99))}")
    print(f"  p99.9 : {fmt_latency(percentile(all_latencies, 99.9))}")
    print(f"  max   : {fmt_latency(all_latencies[-1])}")


def percentile(sorted_data: list[float], p: float) -> float:
    if not sorted_data:
        return 0.0
    idx = min(int(len(sorted_data) * p / 100), len(sorted_data) - 1)
    return sorted_data[idx]


def fmt_latency(seconds: float) -> str:
    if seconds < 1e-6:
        return f"{seconds * 1e9:.0f} ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.1f} µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds:.3f} s"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark lloogg server PUSH throughput and latency.",
    )
    p.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=7379, help="Server port (default: 7379)")
    p.add_argument("-c", "--connections", type=int, default=4, help="Concurrent connections (default: 4)")
    p.add_argument("-n", "--count", type=int, default=50_000, help="Total PUSH requests (default: 50000)")
    p.add_argument("-w", "--warmup", type=int, default=2_000, help="Warmup requests (default: 2000)")
    p.add_argument("--uid", type=int, default=1, help="Base user_id (default: 1)")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(run_benchmark(args))
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()
