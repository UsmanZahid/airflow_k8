"""RunContext — the runtime handed to every step. Resolves Datasets to Delta I/O and owns
the late-arriving-data mechanics: upsert (MERGE), affected-partition/key tracking across
pods, and scoped group recompute.

The SAME code runs against MinIO (`RunContext.from_env`) and local temp dirs
(`RunContext.local`, used by tests) — only the lake URI + storage options differ, so unit
tests exercise real delta-rs semantics without a cluster.
"""

from __future__ import annotations

import contextvars
import datetime as _dt
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

try:  # deltalake exposes this in 1.x; fall back defensively
    from deltalake.exceptions import TableNotFoundError
except Exception:  # pragma: no cover
    class TableNotFoundError(Exception):
        ...

from . import storage
from .catalog import Dataset
from .predicate import predicate_for

_CURRENT: contextvars.ContextVar["RunContext"] = contextvars.ContextVar("current_run")


def current_run() -> "RunContext":
    """Process-global handle to the active run (the 'shared global to the run')."""
    return _CURRENT.get()


def _utcnow() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class RunContext:
    def __init__(self, run_id: str, run_ts: str, logical_date: str,
                 lake_uri: str, storage_options: dict | None):
        self.run_id = run_id
        self.run_ts = run_ts  # identical across all step pods of one DAG run (from {{ ts_nodash }})
        self.logical_date = logical_date
        self._lake = lake_uri.rstrip("/")
        self._so = storage_options or {}
        self._token = None

    # ------------------------------------------------------------------ factories
    @classmethod
    def from_env(cls, run_id: str, run_ts: str, logical_date: str) -> "RunContext":
        import os
        lake = os.environ.get("LAKE_URI", "s3://lake")
        return cls(run_id, run_ts, logical_date, lake, storage.minio_storage_options())

    @classmethod
    def local(cls, base, run_id: str = "local", run_ts: str = "00000000T000000",
              logical_date: str = "2026-01-01") -> "RunContext":
        uri = Path(base).resolve().as_uri()  # file:///... (delta-rs accepts file URIs)
        return cls(run_id, run_ts, logical_date, uri, {})

    def __enter__(self) -> "RunContext":
        self._token = _CURRENT.set(self)
        return self

    def __exit__(self, *exc) -> None:
        _CURRENT.reset(self._token)

    # ------------------------------------------------------------------ uris
    def _uri(self, ds: Dataset) -> str:
        return f"{self._lake}/{ds.rel()}"

    def _marker_uri(self, ds: Dataset, level: str) -> str:
        return f"{self._lake}/_runs/{self.run_ts}/affected/{ds.pipeline}/{ds.table}/{level}"

    def _exists(self, uri: str) -> bool:
        try:
            DeltaTable(uri, storage_options=self._so)
            return True
        except TableNotFoundError:
            return False
        except Exception:
            return False

    # ------------------------------------------------------------------ reads
    def read(self, ds: Dataset) -> pl.DataFrame:
        return pl.read_delta(self._uri(ds), storage_options=self._so)

    def read_new(self, ds: Dataset) -> pl.DataFrame:
        """Only THIS run's slice (avoids reprocessing all bronze history each run)."""
        return self.read(ds).filter(pl.col("run_ts") == self.run_ts)

    def read_groups(self, ds: Dataset, groups: pl.DataFrame, group_cols=None) -> pl.DataFrame:
        cols = list(group_cols or ds.partition_by)
        return self.read(ds).join(groups.select(cols).unique(), on=cols, how="semi")

    # ------------------------------------------------------------------ writes
    def write(self, ds: Dataset, df: pl.DataFrame, mode: str = "overwrite") -> None:
        opts = {"partition_by": list(ds.partition_by)} if ds.partition_by else None
        df.write_delta(self._uri(ds), mode=mode, storage_options=self._so,
                       delta_write_options=opts)

    def write_bronze(self, ds: Dataset, df: pl.DataFrame) -> None:
        """Append raw arrivals, stamped run_ts/ingested_at; idempotent per run (retry-safe)."""
        df = df.with_columns(
            pl.lit(self.run_ts).alias("run_ts"),
            pl.lit(_utcnow()).alias("ingested_at"),
        )
        uri = self._uri(ds)
        if self._exists(uri):
            DeltaTable(uri, storage_options=self._so).delete(f"run_ts = '{self.run_ts}'")
            df.write_delta(uri, mode="append", storage_options=self._so)
        else:
            df.write_delta(uri, mode="overwrite", storage_options=self._so)

    def upsert(self, ds: Dataset, df: pl.DataFrame, source: str = "incremental") -> None:
        """MERGE by ds.key. source='snapshot' also deletes rows absent from the batch.
        Records changed keys and the UNION(pre-image, post-image, deleted) affected partitions.
        """
        if not ds.key:
            raise ValueError(f"{ds.rel()}: upsert requires a key")
        keys = list(ds.key)
        df = df.unique(subset=keys, keep="last")  # merge source must be unique on key
        uri = self._uri(ds)

        # FIRST RUN: merge/pre-image need an existing table -> create, everything is 'affected'.
        if not self._exists(uri):
            self.write(ds, df, mode="overwrite")
            self._record_affected(ds, df)
            return

        pcols = list(ds.partition_by)
        affected_parts = None
        if pcols:
            target = self.read(ds).select(keys + pcols)
            src_keys = df.select(keys)
            matched_pre = target.join(src_keys, on=keys, how="semi").select(pcols)  # updates/moves
            post = df.select(pcols)
            frames = [matched_pre, post]
            if source == "snapshot":  # rows in target but not in batch will be deleted
                deleted = target.join(src_keys, on=keys, how="anti").select(pcols)
                frames.append(deleted)
            affected_parts = pl.concat(frames).unique()

        dt = DeltaTable(uri, storage_options=self._so)
        pred = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        merger = (
            dt.merge(source=df.to_arrow(), predicate=pred, source_alias="s", target_alias="t")
            .when_matched_update_all()
            .when_not_matched_insert_all()
        )
        if source == "snapshot":
            merger = merger.when_not_matched_by_source_delete()
        merger.execute()
        self._record_affected(ds, df, affected_parts)

    def replace_groups(self, ds: Dataset, df: pl.DataFrame, groups: pl.DataFrame, group_cols=None) -> None:
        """Atomically replace ONLY the given groups (replaceWhere). Guards against an empty
        group set (which would overwrite the whole table). Emits an affected marker so a
        multi-hop downstream (gold->gold) sees the changed groups.
        """
        cols = list(group_cols or ds.partition_by)
        if groups.is_empty():
            raise ValueError("empty group set: refusing overwrite (would replace whole table)")
        outside = df.select(cols).unique().join(groups.select(cols).unique(), on=cols, how="anti")
        if not outside.is_empty():
            raise ValueError(f"{ds.rel()}: df has rows outside the target group set {outside.to_dicts()}")

        uri = self._uri(ds)
        if not self._exists(uri):
            self.write(ds, df, mode="overwrite")  # create-if-absent (no predicate on first write)
        else:
            write_deltalake(uri, df.to_arrow(), mode="overwrite",
                            predicate=predicate_for(groups, cols), storage_options=self._so)
        self._record_affected(ds, df)  # MULTI-HOP: emit this dataset's changed groups

    # ------------------------------------------------------------------ change channel
    # Markers are tiny Delta tables under the per-run control dir, so read/write are
    # backend-uniform (file:// and s3:// alike). A missing marker == "nothing changed".
    def _record_affected(self, ds: Dataset, batch: pl.DataFrame, partitions: pl.DataFrame | None = None) -> None:
        if ds.key:
            k = batch.select(list(ds.key)).unique()
            if not k.is_empty():
                k.write_delta(self._marker_uri(ds, "keys"), mode="overwrite", storage_options=self._so)
        if ds.partition_by:
            p = partitions if partitions is not None else batch.select(list(ds.partition_by)).unique()
            p = p.unique()
            if not p.is_empty():
                p.write_delta(self._marker_uri(ds, "partitions"), mode="overwrite", storage_options=self._so)

    def affected(self, ds: Dataset, level: str) -> pl.DataFrame:
        """level in {'keys','partitions'}. Typed-empty frame if no marker was written."""
        uri = self._marker_uri(ds, level)
        cols = list(ds.key) if level == "keys" else list(ds.partition_by)
        if self._exists(uri):
            return pl.read_delta(uri, storage_options=self._so)
        return pl.DataFrame(schema={c: pl.Utf8 for c in cols}) if cols else pl.DataFrame()
