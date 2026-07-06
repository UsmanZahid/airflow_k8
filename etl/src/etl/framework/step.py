"""Class-based Step model. A step's output Dataset is defined once on the class; downstream
steps read it (and its change set) by class reference: `Upstream.read(ctx)` /
`Upstream.affected_partitions(ctx)`.

`AggregateStep` and `MapStep` own the recompute plumbing so authors write only business
logic and invariant #1 (group_cols subset of the source's partition grain) is enforced.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

import polars as pl

from .catalog import Dataset
from .context import RunContext


class Step(ABC):
    id: ClassVar[str]
    output: ClassVar[Dataset | None] = None  # None for sink/publish steps (no Delta output)
    upstream: ClassVar[tuple[type["Step"], ...]] = ()
    resources: ClassVar[dict | None] = None  # optional pod resources -> flows into the manifest

    # --- reading this step's output / change set (called by downstream steps) ---
    @classmethod
    def read(cls, ctx: RunContext) -> pl.DataFrame:
        return ctx.read(cls.output)

    @classmethod
    def read_new(cls, ctx: RunContext) -> pl.DataFrame:
        return ctx.read_new(cls.output)

    @classmethod
    def affected_partitions(cls, ctx: RunContext) -> pl.DataFrame:
        return ctx.affected(cls.output, "partitions")

    @classmethod
    def affected_keys(cls, ctx: RunContext) -> pl.DataFrame:
        return ctx.affected(cls.output, "keys")

    # --- writing this step's output ---
    def write(self, ctx: RunContext, df: pl.DataFrame, mode: str = "overwrite") -> None:
        ctx.write(self.output, df, mode)

    def write_bronze(self, ctx: RunContext, df: pl.DataFrame) -> None:
        ctx.write_bronze(self.output, df)

    def upsert(self, ctx: RunContext, df: pl.DataFrame, source: str = "incremental") -> None:
        ctx.upsert(self.output, df, source)

    def publish_postgres(self, ctx: RunContext, df: pl.DataFrame, table: str, mode: str = "replace") -> None:
        """Sink to the serving Postgres (for BI). Requires SERVING_DB_URI in the pod env."""
        ctx.write_postgres(df, table, mode)

    @abstractmethod
    def run(self, ctx: RunContext) -> None:
        ...


class AggregateStep(Step):
    """Recompute the affected groups of an aggregate. Subclass sets `source_step`,
    `group_cols` (subset of source_step.output.partition_by), and implements `aggregate`."""

    source_step: ClassVar[type[Step]]
    group_cols: ClassVar[tuple[str, ...]]

    @abstractmethod
    def aggregate(self, facts: pl.DataFrame) -> pl.DataFrame:
        ...

    def run(self, ctx: RunContext) -> None:
        cols = list(self.group_cols)
        raw = self.source_step.affected_partitions(ctx)
        if raw.is_empty():
            return
        missing = [c for c in cols if c not in raw.columns]
        if missing:
            raise ValueError(
                f"{self.output.rel()}: group_cols {missing} are not in the source's "
                f"partition grain {raw.columns} (invariant #1 requires group_cols "
                f"subset of source partition_by)"
            )
        groups = raw.select(cols).unique()
        facts = ctx.read_groups(self.source_step.output, groups, cols)
        agg = self.aggregate(facts)
        ctx.replace_groups(self.output, agg, groups, cols)


class MapStep(Step):
    """1:1 / row-level downstream: process only the changed keys, upsert by key."""

    source_step: ClassVar[type[Step]]

    @abstractmethod
    def transform(self, rows: pl.DataFrame) -> pl.DataFrame:
        ...

    def run(self, ctx: RunContext) -> None:
        keys = self.source_step.affected_keys(ctx)
        if keys.is_empty():
            return
        src = self.source_step.output
        rows = ctx.read(src).join(keys.select(list(src.key)).unique(), on=list(src.key), how="semi")
        self.upsert(ctx, self.transform(rows))
