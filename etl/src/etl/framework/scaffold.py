"""Generate a new pipeline package from templates (token substitution in one place)."""

from __future__ import annotations

from pathlib import Path

_STEPS = '''\
from __future__ import annotations

import polars as pl

from etl.framework import AggregateStep, Dataset, Step


class Extract{Cls}(Step):
    id = "extract"
    output = Dataset("bronze", "{name}", "raw")

    def run(self, ctx):
        batch = _fetch(ctx.logical_date)
        self.write_bronze(ctx, batch)


class Clean{Cls}(Step):
    id = "clean"
    source = "snapshot"  # or "incremental"
    output = Dataset("silver", "{name}", "fact", key=("id",), partition_by=("period",))
    upstream = (Extract{Cls},)

    def run(self, ctx):
        df = Extract{Cls}.read_new(ctx).select(
            pl.col("id").cast(pl.Utf8),
            pl.col("period").cast(pl.Utf8),
            pl.col("amount").cast(pl.Float64),
        )
        self.upsert(ctx, df, source=self.source)


class Aggregate{Cls}(AggregateStep):
    id = "aggregate"
    output = Dataset("gold", "{name}", "by_period", partition_by=("period",))
    upstream = (Clean{Cls},)
    source_step = Clean{Cls}
    group_cols = ("period",)

    def aggregate(self, facts):
        return facts.group_by(list(self.group_cols)).agg(pl.col("amount").sum().alias("amount"))


def _fetch(logical_date):
    # TODO: replace with the real source (must be deterministic per logical_date).
    return pl.DataFrame(schema={"id": pl.Utf8, "period": pl.Utf8, "amount": pl.Float64})
'''

_PIPELINE = '''\
from etl.framework import Pipeline, register

from .steps import Aggregate{Cls}, Clean{Cls}, Extract{Cls}


@register
class {Cls}(Pipeline):
    id = "{name}"
    schedule = "@daily"
    steps = (Extract{Cls}, Clean{Cls}, Aggregate{Cls})
    tags = ("etl", "{name}")
'''


def _cls(name: str) -> str:
    return "".join(part.capitalize() for part in name.replace("-", "_").split("_"))


def scaffold(name: str, pipelines_dir: Path) -> Path:
    dest = pipelines_dir / name
    if dest.exists():
        raise FileExistsError(f"pipeline '{name}' already exists at {dest}")
    dest.mkdir(parents=True)
    cls = _cls(name)
    (dest / "__init__.py").write_text("", encoding="utf-8")
    (dest / "steps.py").write_text(_STEPS.format(name=name, Cls=cls), encoding="utf-8")
    (dest / "pipeline.py").write_text(_PIPELINE.format(name=name, Cls=cls), encoding="utf-8")
    return dest
