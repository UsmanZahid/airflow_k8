"""Execute exactly one (pipeline, step) — one step class per pod."""

from __future__ import annotations

from .context import RunContext
from .discovery import load_all
from .pipeline import REGISTRY


def run_step(pipeline_id: str, step_id: str, ctx: RunContext) -> None:
    load_all()
    pipeline = REGISTRY.get(pipeline_id)
    if pipeline is None:
        raise KeyError(f"unknown pipeline '{pipeline_id}' (known: {sorted(REGISTRY)})")
    step_cls = next((s for s in pipeline.steps if s.id == step_id), None)
    if step_cls is None:
        raise KeyError(
            f"unknown step '{step_id}' in pipeline '{pipeline_id}' "
            f"(steps: {[s.id for s in pipeline.steps]})"
        )
    with ctx:
        step_cls().run(ctx)
