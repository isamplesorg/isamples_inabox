import os.path
import json
import csv
from abc import ABC

from isb_lib.utilities.enums import _NoValue


class ExportTransformException(Exception):
    """Exception subclass for when an error occurs during export transform"""


class TargetExportFormat(_NoValue):
    """Valid target export formats"""
    CSV = "csv"


class AbstractExportTransformer(ABC):
    @staticmethod
    def transform(docs: list[dict], fieldnames: list[str], dest_path: str):
        """Transform solr results into a target export format"""
        pass


class CSVExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(docs: list[dict], fieldnames: list[str], dest_path: str):
        if "id" in fieldnames:
            # Manually put id as the first column if it's present
            fieldnames.pop(fieldnames.index("id"))
            fieldnames.insert(0, "id")
        with open(dest_path, "w", newline="") as file:
            writer = csv.DictWriter(
                file,
                delimiter=",",
                quoting=csv.QUOTE_ALL,
                fieldnames=list(fieldnames),
            )
            writer.writeheader()
            for doc in docs:
                writer.writerow(doc)


class SolrResultTransformer:
    def __init__(self, solr_result_path: str, format: TargetExportFormat, transformed_result_path: str):
        self._solr_result_path = solr_result_path
        self._format = format
        self._transformed_result_path = transformed_result_path

    def transform(self):
        if not os.path.exists(self._solr_result_path):
            raise ExportTransformException(f"Source solr result file doesn't exist: {self._solr_result_path}")
        try:
            with open(self._solr_result_path, "r") as file:
                solr_result_dict: dict = json.load(file)
                result_set: dict = solr_result_dict.get("result-set")
                if result_set is not None:
                    docs: list[dict] = result_set.get("docs")
                    if docs is not None and len(docs) > 0:
                        # pick off the solr placeholder if it exists
                        if docs[-1].get("EOF") is not None:
                            docs.pop()
                        fieldnames = list(docs[0].keys())
                        if self._format == TargetExportFormat.CSV:
                            CSVExportTransformer.transform(docs, fieldnames, self._transformed_result_path)
                        else:
                            raise ExportTransformException(f"Unsupported export format: {self._format}")

        except json.JSONDecodeError:
            raise ExportTransformException(f"Source solr result file isn't valid JSON: {self._solr_result_path}")
