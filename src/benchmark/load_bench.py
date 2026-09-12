"""
Load benchmark: validates delivery and measures client-observed p50/p99 latency.

Tests:
  - 1, 5, 10, 20 concurrent clients
  - Each client subscribes to AAPL and TSLA
  - Runs for 15 seconds per scenario
  - Receives and decodes every frame, tracking sequence regressions and errors
  - Computes p50/p99 from combined client-observed receive-latency samples

Run:
  python3 -m src.benchmark.load_bench
"""

import asyncio
import json
import math
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
    received: int
    malformed: int
    sequence_regressions: int
    client_errors: list[str]


@dataclass
class ClientResult:
    client_id: int
    received: int
    malformed: int
    sequence_regressions: int
    latencies_ms: list[float]
    error: str | None = None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(int(len(ordered) * p / 100), len(ordered) - 1)
    return round(ordered[index], 2)


async def run_client(client_id: int, duration: float):
    """Single client: subscribe and receive for `duration` seconds."""
    result = ClientResult(client_id, 0, 0, 0, [])
    deadline = asyncio.get_running_loop().time() + duration
    last_seq: dict[str, int] = {}
    try:
        async with websockets.connect(GATEWAY_WS) as ws:
            for symbol in SYMBOLS:
                await ws.send(json.dumps({"action": "subscribe", "symbol": symbol}))
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                try:
                    message = json.loads(raw)
                    symbol = message.get("symbol")
                    seq = int(message["seq"])
                    previous = last_seq.get(symbol)
                    if previous is not None and seq < previous:
                        result.sequence_regressions += 1
                    last_seq[symbol] = max(seq, previous or seq)
                    event_ts = message.get("event_ts")
                    if event_ts is not None:
                        latency = time.time() * 1000 - int(event_ts)
                        if math.isfinite(latency):
                            result.latencies_ms.append(latency)
                    result.received += 1
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    result.malformed += 1
    except Exception as e:
        result.error = f"client {client_id}: {type(e).__name__}: {e}"
    return result


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
        client_results = await asyncio.gather(*client_tasks)

    errors = [result.error for result in client_results if result.error]
    empty_clients = [
        str(result.client_id) for result in client_results if result.received == 0
    ]
    if empty_clients:
        errors.append(f"clients received no frames: {', '.join(empty_clients)}")
    malformed = sum(result.malformed for result in client_results)
    regressions = sum(result.sequence_regressions for result in client_results)
    if malformed:
        errors.append(f"malformed frames: {malformed}")
    if regressions:
        errors.append(f"sequence regressions: {regressions}")

    latencies = [
        latency
        for result in client_results
        for latency in result.latencies_ms
    ]

    return ScenarioResult(
        n_clients=n_clients,
        p50_ms=percentile(latencies, 50),
        p99_ms=percentile(latencies, 99),
        total_sent=metrics.get("total_sent", 0),
        total_dropped=metrics.get("total_dropped", 0),
        throughput_eps=round(sum(r.received for r in client_results) / DURATION_SEC, 1),
        duration_sec=DURATION_SEC,
        received=sum(r.received for r in client_results),
        malformed=malformed,
        sequence_regressions=regressions,
        client_errors=errors,
    )


def print_result(r: ScenarioResult):
    print(f"  Clients: {r.n_clients:>3} | "
          f"p50: {r.p50_ms:>6.2f}ms | "
          f"p99: {r.p99_ms:>6.2f}ms | "
          f"received: {r.received:>6} | "
          f"dropped: {r.total_dropped} | errors: {len(r.client_errors)}")
    for error in r.client_errors:
        print(f"    ERROR: {error}")


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
        if result.client_errors:
            raise RuntimeError(
                f"Scenario with {n} clients is invalid: "
                + "; ".join(result.client_errors)
            )
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
