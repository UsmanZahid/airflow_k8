"""End-to-end run of the real `costs` Step classes (extract->clean->aggregate) via a local
context, driving the late-arriving-data scenario through the actual pipeline code."""

from __future__ import annotations

import polars as pl

import etl.pipelines.costs.steps as steps
from etl.framework import RunContext
from etl.framework.runner import run_step

_SCHEMA = {"cost_id": pl.Utf8, "cost_period": pl.Utf8, "cost_center": pl.Utf8, "amount": pl.Float64}


def _rows(records):
    cols = list(_SCHEMA)
    return pl.DataFrame({c: [r[i] for r in records] for i, c in enumerate(cols)}, schema=_SCHEMA)


def _run(ctx, batch, monkeypatch):
    monkeypatch.setattr(steps, "fetch_source", lambda logical_date: batch)
    for step in ("extract", "clean", "aggregate"):
        run_step("costs", step, ctx)


def _gold(ctx):
    from etl.pipelines.costs.steps import AggregateCosts
    return {(r["cost_period"], r["cost_center"]): r["amount"]
            for r in ctx.read(AggregateCosts.output).to_dicts()}


def test_costs_end_to_end_late_arriving(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    c1 = RunContext.local(lake, run_id="r1", run_ts="20260101T000000")
    _run(c1, _rows([
        ("A", "2026-05", "X", 100.0), ("B", "2026-05", "X", 50.0), ("C", "2026-06", "X", 200.0),
    ]), monkeypatch)
    assert _gold(c1) == {("2026-05", "X"): 150.0, ("2026-06", "X"): 200.0}

    # snapshot run: A updated, D added late for the old period, C still present
    c2 = RunContext.local(lake, run_id="r2", run_ts="20260102T000000")
    _run(c2, _rows([
        ("A", "2026-05", "X", 130.0), ("B", "2026-05", "X", 50.0),
        ("C", "2026-06", "X", 200.0), ("D", "2026-05", "X", 20.0),
    ]), monkeypatch)
    assert _gold(c2) == {("2026-05", "X"): 200.0, ("2026-06", "X"): 200.0}
