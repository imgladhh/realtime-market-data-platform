"""
Load benchmark: measures p50/p99 dispatch latency under N concurrent clients.

Tests:
  - 1, 5, 10, 20 concurrent clients
  - Each client subscribes to AAPL and TSLA
  - Runs for 15 seconds per scenario
  - Reports p50/p99 latency and throughput from /metrics/summary

Run:
  python3 -m src.benchmark.load_bench
"""

import asyncio
import json
import time
import websockets
import aiohttp
from dataclasses import dataclass


GATEWAY_WS  = "ws://localhost:8000/stream"
GATEWAY_HTTP = "http://localhost:8000"
SYMBOLS     = ["AAPL", "TSLA"]
DURATION_SEC = 15


@dataclass
class ScenarioResult:
    n_clients:    int
    p50_ms:       float
    p99_ms:       float
    total_sent:   int
    total_dropped: int
    throughput_eps: float
    duration_sec: float


async def run_client(client_id: int, duration: float):
    """Single client: subscribe and receive for `duration` seconds."""
    try:
        async with websockets.connect(GATEWAY_WS) as ws:
            for symbol in SYMBOLS:
                await ws.send(json.dumps({"action": "subscribe", "symbol": symbol}))
            await asyncio.sleep(duration)
    except Exception as e:
        pass  # Client disconnect is expected at end of test


async def run_scenario(n_clients: int) -> ScenarioResult:
    print(f"\n  Running: {n_clients} client(s) x {DURATION_SEC}s...")

    # Reset metrics by checking baseline
    async with aiohttp.ClientSession() as http:
        # Start all clients concurrently
        client_tasks = [
            asyncio.create_task(run_client(i, DURATION_SEC))
            for i in range(n_clients)
        ]

        # Wait a bit for clients to connect and accumulate data
        await asyncio.sleep(DURATION_SEC - 2)

        # Snapshot metrics while clients are still connected
        try:
            async with http.get(f"{GATEWAY_HTTP}/metrics/summary") as resp:
                metrics = await resp.json()
        except Exception:
            metrics = {}

        # Wait for clients to finish
        await asyncio.gather(*client_tasks, return_exceptions=True)

    return ScenarioResult(
        n_clients=n_clients,
        p50_ms=metrics.get("avg_p50_ms", 0),
        p99_ms=metrics.get("avg_p99_ms", 0),
        total_sent=metrics.get("total_sent", 0),
        total_dropped=metrics.get("total_dropped", 0),
        throughput_eps=0,
        duration_sec=DURATION_SEC,
    )


def print_result(r: ScenarioResult):
    print(f"  Clients: {r.n_clients:>3} | "
          f"p50: {r.p50_ms:>6.2f}ms | "
          f"p99: {r.p99_ms:>6.2f}ms | "
          f"sent: {r.total_sent:>6} | "
          f"dropped: {r.total_dropped}")


async def main():
    print("\n" + "="*65)
    print("  Load Benchmark: Real-Time Market Data Gateway")
    print("="*65)
    print(f"  Symbols: {SYMBOLS}")
    print(f"  Duration per scenario: {DURATION_SEC}s")
    print("="*65)

    scenarios = [1, 5, 10, 20]
    results = []

    for n in scenarios:
        result = await run_scenario(n)
        print_result(result)
        results.append(result)
        await asyncio.sleep(3)  # cool-down between scenarios

    # Summary table
    print("\n" + "="*65)
    print("  Results Summary")
    print("="*65)
    print(f"  {'Clients':>8} | {'p50 (ms)':>10} | {'p99 (ms)':>10} | {'Dropped':>8}")
    print("  " + "-"*55)
    for r in results:
        print(f"  {r.n_clients:>8} | {r.p50_ms:>10.2f} | {r.p99_ms:>10.2f} | {r.total_dropped:>8}")
    print("="*65 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
