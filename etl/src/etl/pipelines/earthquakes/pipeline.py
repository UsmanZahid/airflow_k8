from etl.framework import Pipeline, register

from .steps import (
    EnrichEarthquakes,
    ExtractEarthquakes,
    NormalizeEarthquakes,
    PublishEarthquakes,
)


@register
class Earthquakes(Pipeline):
    id = "earthquakes"
    schedule = "@daily"
    steps = (ExtractEarthquakes, NormalizeEarthquakes, EnrichEarthquakes, PublishEarthquakes)
    tags = ("etl", "earthquakes", "usgs")
