from etl.framework import Pipeline, register

from .steps import ExtractEarthquakes, NormalizeEarthquakes


@register
class Earthquakes(Pipeline):
    id = "earthquakes"
    schedule = "@daily"
    steps = (ExtractEarthquakes, NormalizeEarthquakes)
    tags = ("etl", "earthquakes", "usgs")
