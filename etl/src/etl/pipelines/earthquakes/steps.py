"""earthquakes pipeline — USGS earthquake monitoring.

Phase 1 (extract -> bronze): fetch the day's geojson from the USGS FDSN event API and append
the raw features. The 24h window is the Airflow data interval [logical_date, logical_date+1),
so runs are deterministic and backfillable (you can replay any historical day).

Phase 2 (normalize -> silver): type-cast, convert epoch-ms to timestamps, derive event_date,
and upsert by the USGS event `id` so revised events (magnitude/status updates) update in place.
Uses source="incremental" — a day's fetch is not a full snapshot, so events outside the window
must NOT be deleted.
"""

from __future__ import annotations

import datetime as dt
import json

import httpx
import polars as pl

from etl.framework import Dataset, MapStep, Step

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Loosely-typed raw landing (normalization/typing happens in silver).
_BRONZE_SCHEMA = {
    "id": pl.Utf8,
    "mag": pl.Float64,
    "place": pl.Utf8,
    "time": pl.Int64,        # epoch milliseconds
    "updated": pl.Int64,     # epoch milliseconds
    "status": pl.Utf8,
    "tsunami": pl.Int64,
    "sig": pl.Int64,
    "net": pl.Utf8,
    "code": pl.Utf8,
    "magtype": pl.Utf8,
    "event_type": pl.Utf8,
    "title": pl.Utf8,
    "longitude": pl.Float64,
    "latitude": pl.Float64,
    "depth": pl.Float64,
    "raw": pl.Utf8,          # full feature JSON for fidelity/audit
}


class ExtractEarthquakes(Step):
    id = "extract"
    output = Dataset("bronze", "earthquakes", "raw")

    def run(self, ctx) -> None:
        start, end = day_window(ctx.logical_date)
        geojson = fetch_geojson(start, end)
        df = features_to_frame(geojson)
        self.write_bronze(ctx, df)


class NormalizeEarthquakes(Step):
    id = "normalize"
    source = "incremental"  # each fetch is new/updated events, NOT a full snapshot -> no deletes
    output = Dataset("silver", "earthquakes", "events", key=("id",), partition_by=("event_date",))
    upstream = (ExtractEarthquakes,)

    def run(self, ctx) -> None:
        raw = ExtractEarthquakes.read_new(ctx)  # only this run's fetched features
        self.upsert(ctx, normalize(raw), source=self.source)


class EnrichEarthquakes(MapStep):
    """Phase 3: enrich the events changed this run with country_code/city (offline reverse
    geocoding from lat/lon) and a significance class. Row-level (MapStep) -> processes only
    the events ingested/revised this run, upserts to gold by id."""

    id = "enrich"
    output = Dataset("gold", "earthquakes", "events_enriched", key=("id",), partition_by=("event_date",))
    upstream = (NormalizeEarthquakes,)
    source_step = NormalizeEarthquakes

    def transform(self, rows: pl.DataFrame) -> pl.DataFrame:
        return enrich(rows)


# --------------------------------------------------------------------------- helpers

def day_window(logical_date: str) -> tuple[str, str]:
    """[start, end) covering the single day of logical_date (24h window)."""
    d = dt.date.fromisoformat(logical_date) if logical_date else dt.date(2014, 1, 1)
    return d.isoformat(), (d + dt.timedelta(days=1)).isoformat()


def fetch_geojson(start: str, end: str) -> dict:
    """GET the USGS FDSN event feed for [start, end) as geojson."""
    resp = httpx.get(
        API_URL,
        params={"format": "geojson", "starttime": start, "endtime": end},
        timeout=60.0,
        follow_redirects=True,
    )
    resp.raise_for_status()
    return resp.json()


def features_to_frame(geojson: dict) -> pl.DataFrame:
    rows = []
    for f in geojson.get("features", []) or []:
        p = f.get("properties") or {}
        coords = (f.get("geometry") or {}).get("coordinates") or []
        rows.append({
            "id": f.get("id"),
            "mag": p.get("mag"),
            "place": p.get("place"),
            "time": p.get("time"),
            "updated": p.get("updated"),
            "status": p.get("status"),
            "tsunami": p.get("tsunami"),
            "sig": p.get("sig"),
            "net": p.get("net"),
            "code": p.get("code"),
            "magtype": p.get("magType"),
            "event_type": p.get("type"),
            "title": p.get("title"),
            "longitude": coords[0] if len(coords) > 0 else None,
            "latitude": coords[1] if len(coords) > 1 else None,
            "depth": coords[2] if len(coords) > 2 else None,
            "raw": json.dumps(f, separators=(",", ":")),
        })
    return pl.DataFrame(rows, schema=_BRONZE_SCHEMA)


def normalize(raw: pl.DataFrame) -> pl.DataFrame:
    """Reshape/rename to the analysis schema; epoch-ms -> timestamps; derive event_date.
    Columns: id, longitude, latitude, elevation, title, place_description, sig, mag,
    magType, time, updated (+ event_date as the partition key)."""
    return (
        raw.with_columns(
            pl.from_epoch(pl.col("time"), time_unit="ms").alias("time"),
            pl.from_epoch(pl.col("updated"), time_unit="ms").alias("updated"),
        )
        .with_columns(pl.col("time").dt.strftime("%Y-%m-%d").alias("event_date"))
        .select(
            "id",
            "longitude",
            "latitude",
            pl.col("depth").alias("elevation"),
            "title",
            pl.col("place").alias("place_description"),
            "sig",
            "mag",
            pl.col("magtype").alias("magType"),
            "time",
            "updated",
            "event_date",  # partition key (additive; not in the base select list)
        )
    )


_GEO = None


def _geocoder():
    """Lazy singleton offline reverse geocoder. mode=1 = single-process (no multiprocessing),
    which is robust under pytest/uv on Windows and fork on Linux alike."""
    global _GEO
    if _GEO is None:
        import reverse_geocoder as rg

        _GEO = rg.RGeocoder(mode=1, verbose=False)
    return _GEO


def enrich(rows: pl.DataFrame) -> pl.DataFrame:
    """Add country_code + city (reverse-geocoded from lat/lon) and sig_class."""
    sig_class = (
        pl.when(pl.col("sig") < 100).then(pl.lit("Low"))
        .when(pl.col("sig") < 500).then(pl.lit("Moderate"))
        .otherwise(pl.lit("High"))
        .alias("sig_class")
    )
    if rows.is_empty():
        return rows.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("country_code"),
            pl.lit(None, dtype=pl.Utf8).alias("city"),
            pl.lit(None, dtype=pl.Utf8).alias("sig_class"),
        )
    coords = [(float(la), float(lo)) for la, lo in zip(rows["latitude"], rows["longitude"])]
    results = _geocoder().query(coords)
    return rows.with_columns(
        pl.Series("country_code", [r.get("cc") for r in results], dtype=pl.Utf8),
        pl.Series("city", [r.get("name") for r in results], dtype=pl.Utf8),
    ).with_columns(sig_class)
