"""Pipeline base + registry. `@register` validates the declaration at import time so
copy-paste footguns (wrong Dataset.pipeline, duplicate step ids, dangling upstream) fail
loudly instead of silently misrouting data."""

from __future__ import annotations

from typing import ClassVar

from .step import Step


class Pipeline:
    id: ClassVar[str]
    schedule: ClassVar[str | None] = None
    steps: ClassVar[tuple[type[Step], ...]] = ()
    tags: ClassVar[tuple[str, ...]] = ()


REGISTRY: dict[str, type[Pipeline]] = {}


def register(pipeline: type[Pipeline]) -> type[Pipeline]:
    _validate(pipeline)
    REGISTRY[pipeline.id] = pipeline
    return pipeline


def _validate(p: type[Pipeline]) -> None:
    assert getattr(p, "id", None), f"{p.__name__}: missing id"
    assert p.steps, f"{p.id}: has no steps"
    ids = [s.id for s in p.steps]
    assert len(ids) == len(set(ids)), f"{p.id}: duplicate step ids {ids}"
    stepset = set(p.steps)
    for s in p.steps:
        assert s.output.pipeline == p.id, (
            f"{p.id}.{s.id}: output.pipeline '{s.output.pipeline}' != pipeline id '{p.id}'"
        )
        for up in s.upstream:
            assert up in stepset, f"{p.id}.{s.id}: upstream {up.__name__} not listed in steps"
