import json

import pytest

from src.benchmark import load_bench


def test_percentile_uses_population_samples():
    assert load_bench.percentile([1.0, 2.0, 100.0], 50) == 2.0
    assert load_bench.percentile([1.0, 2.0, 100.0], 99) == 100.0


@pytest.mark.asyncio
async def test_run_client_receives_and_validates_frames(monkeypatch):
    frames = [
        json.dumps({"type": "snapshot", "symbol": "AAPL", "seq": 10}),
        json.dumps({
            "type": "quote",
            "symbol": "AAPL",
            "seq": 11,
            "event_ts": 1,
        }),
    ]

    class Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def send(self, message):
            pass

        async def recv(self):
            if frames:
                return frames.pop(0)
            await __import__("asyncio").sleep(60)

    monkeypatch.setattr(load_bench.websockets, "connect", lambda url: Connection())

    result = await load_bench.run_client(1, 0.02)

    assert result.received == 2
    assert result.malformed == 0
    assert result.sequence_regressions == 0
    assert result.error is None


@pytest.mark.asyncio
async def test_main_reports_invalid_scenario_reason(monkeypatch):
    async def invalid_scenario(n_clients):
        return load_bench.ScenarioResult(
            n_clients=n_clients,
            p50_ms=0,
            p99_ms=0,
            total_sent=0,
            total_dropped=0,
            throughput_eps=0,
            duration_sec=0,
            received=0,
            malformed=0,
            sequence_regressions=0,
            client_errors=["clients received no frames: 0"],
        )

    monkeypatch.setattr(load_bench, "run_scenario", invalid_scenario)

    with pytest.raises(
        RuntimeError,
        match="Scenario with 1 clients is invalid: clients received no frames: 0",
    ):
        await load_bench.main()
