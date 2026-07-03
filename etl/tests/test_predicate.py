import datetime as dt

import polars as pl
import pytest

from etl.framework.predicate import predicate_for, sql_literal


def test_sql_literal_types():
    assert sql_literal(5) == "5"
    assert sql_literal(3.5) == "3.5"
    assert sql_literal(True) == "true"
    assert sql_literal("X") == "'X'"
    assert sql_literal("a'b") == "'a''b'"  # single-quote escaping
    assert sql_literal(dt.date(2026, 5, 1)) == "'2026-05-01'"
    assert sql_literal(None) is None


def test_predicate_for_or_of_ands():
    df = pl.DataFrame({"cost_period": ["2026-05", "2026-06"], "cost_center": ["X", "Y"]})
    p = predicate_for(df, ["cost_period", "cost_center"])
    assert "(cost_period = '2026-05' AND cost_center = 'X')" in p
    assert "(cost_period = '2026-06' AND cost_center = 'Y')" in p
    assert " OR " in p


def test_predicate_for_empty_raises():
    df = pl.DataFrame({"cost_period": []}, schema={"cost_period": pl.Utf8})
    with pytest.raises(ValueError):
        predicate_for(df, ["cost_period"])
