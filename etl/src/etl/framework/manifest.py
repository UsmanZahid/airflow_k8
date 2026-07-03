"""Export the pipeline registry to the YAML manifest that Airflow's generate_dags.py reads.
The manifest is a generated artifact (never hand-edited); a CI `git diff --exit-code` guard
proves it matches the Python source, preventing DAG/image split-brain."""

from __future__ import annotations

import yaml

from .discovery import load_all
from .pipeline import REGISTRY


def build_manifest(image: str) -> dict:
    load_all()
    pipelines = []
    for pid in sorted(REGISTRY):
        p = REGISTRY[pid]
        steps = []
        for s in p.steps:
            step = {"id": s.id, "upstream": [u.id for u in s.upstream]}
            if s.resources:
                step["resources"] = s.resources
            steps.append(step)
        pipelines.append({
            "id": p.id,
            "schedule": p.schedule,
            "image": image,
            "tags": list(p.tags),
            "steps": steps,
        })
    return {"pipelines": pipelines}


def export_manifest(image: str, out: str) -> str:
    manifest = build_manifest(image)
    with open(out, "w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f, sort_keys=False)
    return out
