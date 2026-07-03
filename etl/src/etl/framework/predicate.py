"""Typed SQL predicate builder for delta-rs replaceWhere / partition overwrite.

delta-rs requires correctly typed & quoted literals or partition pruning silently breaks
(or the write errors). This is the ONE place predicates are built, so it can be unit-tested
against every partition dtype.
"""

from __future__ import annotations

import datetime as _dt

import polars as pl


def sql_literal(v) -> str | None:
    """Render a Python value as a SQL literal, or None to signal `IS NULL`."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (_dt.date, _dt.datetime)):
        return f"'{v.isoformat()}'"
    s = str(v).replace("'", "''")  # escape single quotes
    return f"'{s}'"


def _eq(col: str, v) -> str:
    lit = sql_literal(v)
    return f"{col} IS NULL" if lit is None else f"{col} = {lit}"


def predicate_for(groups: pl.DataFrame, cols: list[str]) -> str:
    """OR-of-ANDs predicate selecting exactly the given group tuples."""
    rows = groups.select(cols).unique().to_dicts()
    if not rows:
        raise ValueError("empty group set: refusing to build predicate (would match nothing/everything)")
    ors = [f"({' AND '.join(_eq(c, r[c]) for c in cols)})" for r in rows]
    return " OR ".join(ors)
