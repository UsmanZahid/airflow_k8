"""Blocker-case coverage against real delta-rs via RunContext.local (no cluster).

Covers: first-run create, update-in-place, late-arriving aggregate correction,
incremental scoping (untouched groups not recomputed), snapshot delete, partition-move
(old+new recompute), empty-run no-op, and multi-hop marker emission.
"""

from __future__ import annotations

import polars as pl
import pytest

from etl.framework import Dataset, RunContext

SILVER = Dataset("silver", "costs", "fact", key=("cost_id",), partition_by=("cost_period", "cost_center"))
GOLD = Dataset("gold", "costs", "cost_by_period", partition_by=("cost_period", "cost_center"))
GCOLS = ["cost_period", "cost_center"]
_SCHEMA = {"cost_id": pl.Utf8, "cost_period": pl.Utf8, "cost_center": pl.Utf8, "amount": pl.Float64}


def rc(lake, n: int) -> RunContext:
    return RunContext.local(lake, run_id=f"r{n}", run_ts=f"2026010{n}T000000")


def rows(records: list[tuple]) -> pl.DataFrame:
    cols = list(_SCHEMA)
    data = {c: [r[i] for r in records] for i, c in enumerate(cols)}
    return pl.DataFrame(data, schema=_SCHEMA)


def aggregate(ctx: RunContext) -> None:
    groups = ctx.affected(SILVER, "partitions").select(GCOLS).unique()
    if groups.is_empty():
        return
    facts = ctx.read_groups(SILVER, groups, GCOLS)
    agg = facts.group_by(GCOLS).agg(pl.col("amount").sum().alias("amount"))
    ctx.replace_groups(GOLD, agg, groups, GCOLS)


def gold_map(ctx: RunContext) -> dict:
    return {(r["cost_period"], r["cost_center"]): r["amount"] for r in ctx.read(GOLD).to_dicts()}


def test_first_run_creates_and_aggregates(tmp_path):
    lake = tmp_path / "lake"
    c1 = rc(lake, 1)
    with c1:
        c1.upsert(SILVER, rows([("A", "2026-05", "X", 100.0), ("B", "2026-05", "X", 50.0)]), source="snapshot")
        aggregate(c1)
    assert gold_map(c1)[("2026-05", "X")] == 150.0


def test_update_in_place_one_row_per_key(tmp_path):
    lake = tmp_path / "lake"
    with rc(lake, 1) as c1:
        c1.upsert(SILVER, rows([("A", "2026-05", "X", 100.0)]), source="incremental")
    with rc(lake, 2) as c2:
        c2.upsert(SILVER, rows([("A", "2026-05", "X", 130.0)]), source="incremental")
        silver = c2.read(SILVER)
    assert silver.height == 1 and silver["amount"][0] == 130.0


def test_late_arriving_correction(tmp_path):
    lake = tmp_path / "lake"
    with rc(lake, 1) as c1:
        c1.upsert(SILVER, rows([
            ("A", "2026-05", "X", 100.0), ("B", "2026-05", "X", 50.0), ("C", "2026-06", "X", 200.0),
        ]), source="incremental")
        aggregate(c1)
    assert gold_map(c1) == {("2026-05", "X"): 150.0, ("2026-06", "X"): 200.0}

    # A updated 100->130, late D=20 for the old period; 06 untouched (incremental batch)
    with rc(lake, 2) as c2:
        c2.upsert(SILVER, rows([("A", "2026-05", "X", 130.0), ("D", "2026-05", "X", 20.0)]), source="incremental")
        affected = {tuple(r) for r in c2.affected(SILVER, "partitions").select(GCOLS).unique().rows()}
        assert affected == {("2026-05", "X")}  # scoped: 06 not affected
        aggregate(c2)
    assert gold_map(c2) == {("2026-05", "X"): 200.0, ("2026-06", "X"): 200.0}


def test_snapshot_delete_propagates(tmp_path):
    lake = tmp_path / "lake"
    with rc(lake, 1) as c1:
        c1.upsert(SILVER, rows([("A", "2026-05", "X", 100.0), ("B", "2026-05", "X", 50.0)]), source="snapshot")
        aggregate(c1)
    assert gold_map(c1)[("2026-05", "X")] == 150.0

    with rc(lake, 2) as c2:
        c2.upsert(SILVER, rows([("A", "2026-05", "X", 100.0)]), source="snapshot")  # B removed
        aggregate(c2)
        silver_ids = set(c2.read(SILVER)["cost_id"].to_list())
    assert silver_ids == {"A"}
    assert gold_map(c2)[("2026-05", "X")] == 100.0


def test_partition_move_recomputes_old_and_new(tmp_path):
    lake = tmp_path / "lake"
    with rc(lake, 1) as c1:
        c1.upsert(SILVER, rows([("A", "2026-05", "X", 100.0), ("C", "2026-04", "X", 5.0)]), source="snapshot")
        aggregate(c1)
    assert gold_map(c1) == {("2026-05", "X"): 100.0, ("2026-04", "X"): 5.0}

    # A's period corrected 2026-05 -> 2026-04 (row moves partitions)
    with rc(lake, 2) as c2:
        c2.upsert(SILVER, rows([("A", "2026-04", "X", 100.0), ("C", "2026-04", "X", 5.0)]), source="snapshot")
        affected = {tuple(r) for r in c2.affected(SILVER, "partitions").select(GCOLS).unique().rows()}
        assert affected == {("2026-05", "X"), ("2026-04", "X")}  # both marked
        aggregate(c2)
        g = gold_map(c2)
    assert g.get(("2026-04", "X")) == 105.0
    assert ("2026-05", "X") not in g  # old group emptied


def test_empty_run_is_noop(tmp_path):
    lake = tmp_path / "lake"
    with rc(lake, 1) as c1:
        c1.upsert(SILVER, rows([("A", "2026-05", "X", 100.0)]), source="incremental")
        aggregate(c1)
    before = gold_map(c1)
    with rc(lake, 2) as c2:
        assert c2.affected(SILVER, "partitions").is_empty()  # nothing upserted -> no marker
        aggregate(c2)  # must no-op, not overwrite the whole table
    assert gold_map(c2) == before


def test_multihop_marker_emitted(tmp_path):
    lake = tmp_path / "lake"
    with rc(lake, 1) as c1:
        c1.upsert(SILVER, rows([("A", "2026-05", "X", 100.0)]), source="incremental")
        aggregate(c1)
        m = {tuple(r) for r in c1.affected(GOLD, "partitions").select(GCOLS).unique().rows()}
    assert m == {("2026-05", "X")}  # gold replace emitted a marker for the next hop
