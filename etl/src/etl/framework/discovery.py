"""Import every pipeline package's `pipeline` module so its Pipeline subclass registers.
Packages whose name starts with `_` (e.g. `_template`) are skipped."""

from __future__ import annotations

import importlib
import pkgutil

_loaded = False


def load_all() -> None:
    global _loaded
    if _loaded:
        return
    import etl.pipelines as pkg

    errors: dict[str, str] = {}
    for mod in pkgutil.iter_modules(pkg.__path__):
        if mod.name.startswith("_"):
            continue
        try:
            importlib.import_module(f"etl.pipelines.{mod.name}.pipeline")
        except Exception as exc:  # collect all, report together (dev ergonomics)
            errors[mod.name] = f"{type(exc).__name__}: {exc}"
    if errors:
        detail = "\n".join(f"  - {k}: {v}" for k, v in errors.items())
        raise ImportError(f"failed to import pipeline module(s):\n{detail}")
    _loaded = True
