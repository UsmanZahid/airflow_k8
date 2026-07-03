"""A Dataset is a named handle to a persistent Delta table in the medallion lake.

The path/identity is declared ONCE here (on the producing step's class); downstream steps
reference the class, never a hardcoded path. `key` enables upsert (MERGE); `partition_by`
is the business/aggregation dimension used for scoped recompute (never the record id).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    layer: str  # bronze | silver | gold
    pipeline: str
    table: str
    key: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()

    def rel(self) -> str:
        return f"{self.layer}/{self.pipeline}/{self.table}"
