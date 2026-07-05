from etl.framework import Pipeline, register

from .steps import EnrichEarthquakes, ExtractEarthquakes, NormalizeEarthquakes


@register
class Earthquakes(Pipeline):
    id = "earthquakes"
    schedule = "@daily"
    steps = (ExtractEarthquakes, NormalizeEarthquakes, EnrichEarthquakes)
    tags = ("etl", "earthquakes", "usgs")
