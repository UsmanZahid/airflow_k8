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

# Bronze is the raw feature JSON, untouched (`id` kept as the natural key for readability).
# All flattening/typing happens in normalize (phase 2).
_BRONZE_SCHEMA = {"id": pl.Utf8, "raw": pl.Utf8}

# Schema used to decode the raw feature JSON in normalize (only the fields we need; extra
# USGS fields are ignored). Mirrors the geojson shape: {id, properties{...}, geometry{...}}.
_FEATURE_DTYPE = pl.Struct({
    "id": pl.Utf8,
    "properties": pl.Struct({
        "mag": pl.Float64,
        "place": pl.Utf8,
        "time": pl.Int64,       # epoch milliseconds
        "updated": pl.Int64,    # epoch milliseconds
        "sig": pl.Int64,
        "magType": pl.Utf8,
        "title": pl.Utf8,
    }),
    "geometry": pl.Struct({"coordinates": pl.List(pl.Float64)}),  # [lon, lat, depth]
})


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
    """Bronze landing: one row per feature holding the raw JSON, untouched."""
    feats = geojson.get("features", []) or []
    return pl.DataFrame(
        {
            "id": [f.get("id") for f in feats],
            "raw": [json.dumps(f, separators=(",", ":")) for f in feats],
        },
        schema=_BRONZE_SCHEMA,
    )


def normalize(raw: pl.DataFrame) -> pl.DataFrame:
    """Parse the raw feature JSON and reshape to the analysis schema via Polars struct access
    (mirrors Spark's col('properties.mag') / col('geometry.coordinates').getItem(i)).
    Columns: id, longitude, latitude, elevation, title, place_description, sig, mag,
    magType, time, updated (+ event_date partition key). epoch-ms -> timestamps."""
    decoded = raw.with_columns(pl.col("raw").str.json_decode(_FEATURE_DTYPE).alias("f"))
    props = pl.col("f").struct.field("properties")
    coords = pl.col("f").struct.field("geometry").struct.field("coordinates")
    return (
        decoded.select(
            pl.col("f").struct.field("id").alias("id"),
            coords.list.get(0, null_on_oob=True).alias("longitude"),
            coords.list.get(1, null_on_oob=True).alias("latitude"),
            coords.list.get(2, null_on_oob=True).alias("elevation"),
            props.struct.field("title").alias("title"),
            props.struct.field("place").alias("place_description"),
            props.struct.field("sig").alias("sig"),
            props.struct.field("mag").alias("mag"),
            props.struct.field("magType").alias("magType"),
            props.struct.field("time").alias("time"),
            props.struct.field("updated").alias("updated"),
        )
        .with_columns(
            pl.from_epoch(pl.col("time"), time_unit="ms").alias("time"),
            pl.from_epoch(pl.col("updated"), time_unit="ms").alias("updated"),
        )
        .with_columns(pl.col("time").dt.strftime("%Y-%m-%d").alias("event_date"))
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
