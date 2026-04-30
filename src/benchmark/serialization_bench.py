"""
Serialization benchmark: JSON vs msgpack

Measures:
  - encode time (server side: serialize event before sending)
  - decode time (client side: deserialize received bytes)
  - payload size (bytes on the wire)

Run:
  python3 -m src.benchmark.serialization_bench
"""

import json
import time
import msgpack
from src.models import MarketEvent, EventType

ITERATIONS = 100_000


def make_sample_event() -> dict:
    return MarketEvent(
        symbol="AAPL",
        bid=189.11,
        ask=189.13,
        bid_size=500,
        ask_size=300,
        event_ts=1710001234567,
        server_ts=1710001234571,
        seq=102938,
        type=EventType.QUOTE,
    ).to_dict()


def bench_json(event: dict, n: int) -> dict:
    # Encode
    start = time.perf_counter()
    for _ in range(n):
        encoded = json.dumps(event).encode()
    encode_time = time.perf_counter() - start

    # Decode
    start = time.perf_counter()
    for _ in range(n):
        json.loads(encoded)
    decode_time = time.perf_counter() - start

    return {
        "encode_ms":   round(encode_time * 1000, 2),
        "decode_ms":   round(decode_time * 1000, 2),
        "total_ms":    round((encode_time + decode_time) * 1000, 2),
        "payload_bytes": len(encoded),
        "encode_per_sec": int(n / encode_time),
        "decode_per_sec": int(n / decode_time),
    }


def bench_msgpack(event: dict, n: int) -> dict:
    # Encode
    start = time.perf_counter()
    for _ in range(n):
        encoded = msgpack.packb(event, use_bin_type=True)
    encode_time = time.perf_counter() - start

    # Decode
    start = time.perf_counter()
    for _ in range(n):
        msgpack.unpackb(encoded, raw=False)
    decode_time = time.perf_counter() - start

    return {
        "encode_ms":   round(encode_time * 1000, 2),
        "decode_ms":   round(decode_time * 1000, 2),
        "total_ms":    round((encode_time + decode_time) * 1000, 2),
        "payload_bytes": len(encoded),
        "encode_per_sec": int(n / encode_time),
        "decode_per_sec": int(n / decode_time),
    }


def print_results(name: str, results: dict):
    print(f"\n{'='*40}")
    print(f"  {name}")
    print(f"{'='*40}")
    print(f"  Payload size:      {results['payload_bytes']} bytes")
    print(f"  Encode time:       {results['encode_ms']} ms ({results['encode_per_sec']:,} ops/sec)")
    print(f"  Decode time:       {results['decode_ms']} ms ({results['decode_per_sec']:,} ops/sec)")
    print(f"  Total (enc+dec):   {results['total_ms']} ms")


def main():
    event = make_sample_event()
    n = ITERATIONS

    print(f"\nSerialization Benchmark: {n:,} iterations")
    print(f"Event: {event}")

    json_r    = bench_json(event, n)
    msgpack_r = bench_msgpack(event, n)

    print_results("JSON", json_r)
    print_results("msgpack", msgpack_r)

    # Summary
    size_reduction = round(
        (1 - msgpack_r["payload_bytes"] / json_r["payload_bytes"]) * 100, 1
    )
    speed_improvement = round(
        json_r["total_ms"] / msgpack_r["total_ms"], 2
    )

    print(f"\n{'='*40}")
    print(f"  Summary")
    print(f"{'='*40}")
    print(f"  Payload size reduction: {size_reduction}%")
    print(f"  Speed improvement:      {speed_improvement}x faster")
    print(f"{'='*40}\n")


if __name__ == "__main__":
    main()
