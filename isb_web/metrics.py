import logging
from typing import Optional

from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from isb_web.sqlmodel_database import SQLModelDAO

metrics_api = FastAPI()
dao: Optional[SQLModelDAO] = None
METRICS_PREFIX = "/metrics"
logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("metrics")


class PrometheusMetrics:
    db_counts: dict[str: int]
    db_scrape_duration_seconds: float
    solr_counts: dict[str: int]
    solr_scrape_duration_seconds: float

    def metrics_string(self) -> str:
        metrics_lines: list[str] = []
        """Returns the metrics in the prometheus format"""
        for db_key, db_value in self.db_counts.items():
            authority_metric_name = db_key.lower()
            # First generate the documentation and type definition for the field
            metrics_lines.append(f"# HELP isamples_{authority_metric_name}_thing_count A count of things from authority {db_key}.")
            metrics_lines.append(f"# TYPE isamples_{authority_metric_name}_thing_count gauge")
            # Then generate the value
            metrics_lines.append(f"isamples_{authority_metric_name}_thing_count {db_value}")
            metrics_lines.append("\n")
        metrics_lines.append("# HELP isamples_db_scrape_duration_seconds How long it took to do the db scrape.")
        metrics_lines.append("# TYPE isamples_db_scrape_duration_seconds gauge")
        metrics_lines.append(f"isamples_db_scrape_duration_seconds {self.db_scrape_duration_seconds}")
        return "\n".join(metrics_lines)


@metrics_api.get("/")
def root():
    metrics = PrometheusMetrics()
    metrics.db_counts = {
        "OPENCONTEXT": 1,
        "SESAR": 2,
        "SMITHSONIAN": 3,
        "GEOME": 4
    }
    metrics.db_scrape_duration_seconds = 99.9
    return PlainTextResponse(metrics.metrics_string())
