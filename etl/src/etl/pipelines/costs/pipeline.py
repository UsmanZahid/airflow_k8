from etl.framework import Pipeline, register

from .steps import AggregateCosts, CleanCosts, ExtractCosts


@register
class Costs(Pipeline):
    id = "costs"
    schedule = "@daily"
    steps = (ExtractCosts, CleanCosts, AggregateCosts)
    tags = ("etl", "costs")
