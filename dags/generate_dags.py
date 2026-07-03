"""Render one Airflow DAG per pipeline from the generated manifest.

Written ONCE. Adding a pipeline never touches this file — you add an etl/ folder and
regenerate the manifest. The only non-stdlib import is PyYAML (ships with Airflow) plus the
cncf.kubernetes provider (bundled with the KubernetesExecutor image).
"""

from __future__ import annotations

import os

import pendulum
import yaml
from airflow import DAG

from common.kpo import etl_step_task

_MANIFEST = os.path.join(os.path.dirname(__file__), "pipelines.generated.yaml")

with open(_MANIFEST, encoding="utf-8") as f:
    _manifest = yaml.safe_load(f) or {"pipelines": []}

for _p in _manifest["pipelines"]:
    _dag = DAG(
        dag_id=_p["id"],
        schedule=_p.get("schedule"),
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,  # serialize writers to shared Delta tables (correctness guard)
        tags=_p.get("tags") or [],
        params={"full": False},
    )
    with _dag:
        _tasks = {
            _s["id"]: etl_step_task(_p["id"], _s["id"], image=_p["image"], resources=_s.get("resources"))
            for _s in _p["steps"]
        }
        for _s in _p["steps"]:
            for _up in _s.get("upstream", []):
                _tasks[_up] >> _tasks[_s["id"]]

    globals()[_p["id"]] = _dag  # Airflow discovers DAGs in module globals
