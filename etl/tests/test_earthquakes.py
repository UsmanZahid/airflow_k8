"""earthquakes pipeline tests — mocked USGS fetch (no network), via LocalRunContext.

Covers: geojson -> normalized silver, event_date derivation, upsert-by-id revision
(one row per event, revised in place), and incremental no-delete across days.
"""

from __future__ import annotations

import datetime as dt

import etl.pipelines.earthquakes.steps as eq
from etl.framework import RunContext
from etl.framework.runner import run_step


def _ms(y, mo, d, h=12) -> int:
    return int(dt.datetime(y, mo, d, h, tzinfo=dt.timezone.utc).timestamp() * 1000)


def _feature(fid, mag, time_ms, updated_ms, lon=-116.0, lat=33.0, depth=10.0, place="X"):
    return {
        "type": "Feature", "id": fid,
        "properties": {
            "mag": mag, "place": place, "time": time_ms, "updated": updated_ms,
            "status": "reviewed", "tsunami": 0, "sig": 10, "net": "ci", "code": fid,
            "magType": "ml", "type": "earthquake", "title": f"M {mag} - {place}",
        },
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
    }


def _geojson(*feats):
    return {"type": "FeatureCollection", "metadata": {"count": len(feats)}, "features": list(feats)}


def _rc(lake, n):
    return RunContext.local(lake, run_id=f"r{n}", run_ts=f"2026020{n}T000000", logical_date=f"2014-01-0{n}")


def _run(ctx, gj, monkeypatch):
    monkeypatch.setattr(eq, "fetch_geojson", lambda start, end: gj)
    for step in ("extract", "normalize"):
        run_step("earthquakes", step, ctx)


def _silver(ctx):
    return ctx.read(eq.NormalizeEarthquakes.output)


def test_day_window():
    assert eq.day_window("2014-01-01") == ("2014-01-01", "2014-01-02")


def test_normalize_shapes_and_event_date(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    gj = _geojson(
        _feature("ci1", 1.5, _ms(2014, 1, 1), _ms(2014, 1, 1), lon=-116.7, lat=33.6, depth=11.0, place="CA"),
        _feature("ci2", 2.1, _ms(2014, 1, 1, 3), _ms(2014, 1, 1, 3)),
    )
    _run(_rc(lake, 1), gj, monkeypatch)
    s = _silver(_rc(lake, 1)).sort("id")
    assert s.height == 2
    assert s["id"].to_list() == ["ci1", "ci2"]
    assert set(s["event_date"].to_list()) == {"2014-01-01"}
    # analysis schema (their phase-2 spec) + event_date partition
    assert set(s.columns) == {
        "id", "longitude", "latitude", "elevation", "title", "place_description",
        "sig", "mag", "magType", "time", "updated", "event_date",
    }
    row = s.filter(s["id"] == "ci1").to_dicts()[0]
    assert row["mag"] == 1.5 and row["longitude"] == -116.7 and row["elevation"] == 11.0
    assert row["place_description"] == "CA" and row["magType"] == "ml"
    assert row["time"] == dt.datetime(2014, 1, 1, 12)


def test_upsert_revision_updates_in_place(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    _run(_rc(lake, 1), _geojson(_feature("ci1", 2.0, _ms(2014, 1, 1), _ms(2014, 1, 1))), monkeypatch)
    # next run re-reports ci1 with a revised magnitude, plus a new event ci2 (same day)
    _run(_rc(lake, 2), _geojson(
        _feature("ci1", 2.5, _ms(2014, 1, 1), _ms(2014, 1, 2)),   # revised
        _feature("ci2", 3.0, _ms(2014, 1, 1, 6), _ms(2014, 1, 2)),
    ), monkeypatch)
    s = _silver(_rc(lake, 2)).sort("id")
    assert s["id"].to_list() == ["ci1", "ci2"]              # one row per id
    assert s.filter(s["id"] == "ci1")["mag"][0] == 2.5      # revised in place


def test_incremental_does_not_delete_prior_days(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    _run(_rc(lake, 1), _geojson(_feature("ci1", 1.0, _ms(2014, 1, 1), _ms(2014, 1, 1))), monkeypatch)
    # day 2 fetch contains only day-2 events; ci1 must NOT be deleted (source=incremental)
    _run(_rc(lake, 2), _geojson(_feature("ci2", 4.0, _ms(2014, 1, 2), _ms(2014, 1, 2))), monkeypatch)
    s = _silver(_rc(lake, 2)).sort("id")
    assert s["id"].to_list() == ["ci1", "ci2"]
    assert set(s["event_date"].to_list()) == {"2014-01-01", "2014-01-02"}


def _with_sig(feat, sig):
    feat["properties"]["sig"] = sig
    return feat


def test_enrich_adds_country_code_and_sig_class(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    gj = _geojson(
        _with_sig(_feature("us1", 3.0, _ms(2014, 1, 1), _ms(2014, 1, 1), lon=-118.25, lat=34.05, place="LA"), 50),
        _with_sig(_feature("jp1", 4.0, _ms(2014, 1, 1, 2), _ms(2014, 1, 1, 2), lon=139.69, lat=35.68, place="Tokyo"), 200),
        _with_sig(_feature("vu1", 6.5, _ms(2014, 1, 1, 4), _ms(2014, 1, 1, 4), lon=167.29, lat=-13.09, place="Vanuatu"), 600),
    )
    ctx = _rc(lake, 1)
    monkeypatch.setattr(eq, "fetch_geojson", lambda s, e: gj)
    for step in ("extract", "normalize", "enrich"):
        run_step("earthquakes", step, ctx)

    g = {r["id"]: r for r in ctx.read(eq.EnrichEarthquakes.output).to_dicts()}
    assert g["us1"]["country_code"] == "US" and g["us1"]["sig_class"] == "Low"
    assert g["jp1"]["country_code"] == "JP" and g["jp1"]["sig_class"] == "Moderate"
    assert g["vu1"]["country_code"] == "VU" and g["vu1"]["sig_class"] == "High"
