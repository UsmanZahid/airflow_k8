"""costs pipeline — the exemplar exercising late-arriving update propagation.

extract (append raw)  ->  clean (upsert by cost_id, snapshot deletes)  ->
aggregate (recompute only affected (cost_period, cost_center) groups).
"""

from __future__ import annotations

import polars as pl

from etl.framework import AggregateStep, Dataset, Step

_SCHEMA = {
    "cost_id": pl.Utf8,
    "cost_period": pl.Utf8,
    "cost_center": pl.Utf8,
    "amount": pl.Float64,
}


class ExtractCosts(Step):
    id = "extract"
    output = Dataset("bronze", "costs", "raw")

    def run(self, ctx) -> None:
        batch = fetch_source(ctx.logical_date)  # may include late / updated / removed rows
        self.write_bronze(ctx, batch)


class CleanCosts(Step):
    id = "clean"
    source = "snapshot"  # each run is a full snapshot -> merge also applies deletes
    output = Dataset(
        "silver", "costs", "fact",
        key=("cost_id",),
        partition_by=("cost_period", "cost_center"),
    )
    upstream = (ExtractCosts,)

    def run(self, ctx) -> None:
        df = ExtractCosts.read_new(ctx).pipe(normalize)  # only THIS run's arrivals
        self.upsert(ctx, df, source=self.source)


class AggregateCosts(AggregateStep):
    id = "aggregate"
    output = Dataset("gold", "costs", "cost_by_period", partition_by=("cost_period", "cost_center"))
    upstream = (CleanCosts,)
    source_step = CleanCosts
    group_cols = ("cost_period", "cost_center")
    resources = {"requests": {"cpu": "250m", "memory": "512Mi"},
                 "limits": {"cpu": "1", "memory": "1Gi"}}

    def aggregate(self, facts: pl.DataFrame) -> pl.DataFrame:
        return facts.group_by(list(self.group_cols)).agg(pl.col("amount").sum().alias("amount"))


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col("cost_id").cast(pl.Utf8),
        pl.col("cost_period").cast(pl.Utf8),
        pl.col("cost_center").cast(pl.Utf8),
        pl.col("amount").cast(pl.Float64),
    )


def fetch_source(logical_date: str) -> pl.DataFrame:
    """Source of raw cost rows. Real impl hits an external system, bounded by logical_date so
    a retry re-fetches the SAME batch (idempotent bronze).

    For local/in-cluster demos, a batch may be injected as JSON via COSTS_SEED_JSON, e.g.
    `[{"cost_id":"A","cost_period":"2026-05","cost_center":"X","amount":100}]`.
    """
    import json
    import os

    seed = os.environ.get("COSTS_SEED_JSON")
    if seed:
        return pl.DataFrame(json.loads(seed), schema=_SCHEMA)
    return pl.DataFrame(schema=_SCHEMA)
