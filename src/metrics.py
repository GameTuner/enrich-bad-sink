from opencensus.ext.stackdriver import stats_exporter
from opencensus.stats import aggregation
from opencensus.stats import measure
from opencensus.stats import stats
from opencensus.stats import view

def add_metric(metric_name: str, metric_description: str, metric_unit: str, aggregation_type: any):
    MEASURE = measure.MeasureInt(
        metric_name,
        metric_description,
        metric_unit)
    VIEW = view.View(
        metric_name,
        metric_description,
        [],
        MEASURE,
        aggregation_type)
    stats.stats.view_manager.register_view(VIEW)

    return MEASURE

def record_metric(metric: measure.MeasureInt, value: int):
    mmap = stats.stats.stats_recorder.new_measurement_map()
    mmap.measure_int_put(metric, value)
    mmap.record()

def start_exporter():
    exporter = stats_exporter.new_stats_exporter(interval=10)
    stats.stats.view_manager.register_exporter(exporter)