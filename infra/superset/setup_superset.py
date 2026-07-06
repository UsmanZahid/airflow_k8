"""Reproducibly create the Superset serving-DB connection, the earthquakes dataset, and the
country World Map chart via the REST API.

Two ways to recreate the BI assets from code:
  1. Native import (preferred):
       superset import-directory infra/superset/assets   # inside the superset pod
     or via the UI: Settings -> Import dashboards/charts (the assets/ bundle).
  2. Run this script against a port-forwarded Superset:
       kubectl -n superset port-forward svc/superset 8088:8088
       uv run --project etl python infra/superset/setup_superset.py

Requires httpx (in the etl venv). Admin creds default to admin/admin.
"""

from __future__ import annotations

import json
import os

import httpx

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SERVING_URI = "postgresql://bi:bi_password@serving-postgres.superset.svc.cluster.local:5432/analytics"


def main() -> None:
    c = httpx.Client(base_url=BASE, timeout=60)
    tok = c.post("/api/v1/security/login",
                 json={"username": "admin", "password": "admin", "provider": "db", "refresh": True}).json()["access_token"]
    c.headers["Authorization"] = f"Bearer {tok}"
    c.headers["X-CSRFToken"] = c.get("/api/v1/security/csrf_token/").json()["result"]
    c.headers["Referer"] = BASE + "/"

    dbid = c.post("/api/v1/database/", json={
        "database_name": "serving", "sqlalchemy_uri": SERVING_URI,
    }).json()["id"]

    dsid = c.post("/api/v1/dataset/", json={
        "database": dbid, "schema": "public", "table_name": "earthquake_events",
    }).json()["id"]

    params = {
        "viz_type": "world_map", "datasource": f"{dsid}__table",
        "entity": "country_code", "country_fieldtype": "cca2", "metric": "count",
        "adhoc_filters": [], "row_limit": 1000, "max_bubble_size": "25", "show_bubbles": True,
    }
    cid = c.post("/api/v1/chart/", json={
        "slice_name": "Earthquake events by country", "viz_type": "world_map",
        "datasource_id": dsid, "datasource_type": "table", "params": json.dumps(params),
    }).json()["id"]

    print(f"created database={dbid} dataset={dsid} chart={cid}")
    print(f"open: {BASE}/explore/?slice_id={cid}")


if __name__ == "__main__":
    main()
