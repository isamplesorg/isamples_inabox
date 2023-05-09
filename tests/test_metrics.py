from isb_web.metrics import PrometheusMetrics


def test_prometheus_metrics():
    metrics = PrometheusMetrics()
    metrics.solr_counts = {"GEOME": 1, "OPENCONTEXT": 2}
    metrics.solr_scrape_duration_seconds = 5.0
    metrics.db_counts = {"GEOME": 2, "OPENCONTEXT": 1}
    metrics.db_scrape_duration_seconds = 10.0
    metrics_string = metrics.metrics_string()
    assert len(metrics_string) > 0