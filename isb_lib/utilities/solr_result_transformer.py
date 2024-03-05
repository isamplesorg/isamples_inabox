import csv
from abc import ABC

import petl
from petl import Table

from isamples_metadata.metadata_constants import METADATA_PLACE_NAME
from isamples_metadata.solr_field_constants import SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME
from isb_lib.utilities.enums import _NoValue


class ExportTransformException(Exception):
    """Exception subclass for when an error occurs during export transform"""


class TargetExportFormat(_NoValue):
    """Valid target export formats"""
    CSV = "csv"
    JSON = "json"


class AbstractExportTransformer(ABC):
    @staticmethod
    def transform(table: Table, dest_path: str):
        """Transform solr results into a target export format"""
        pass


class CSVExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(table: Table, dest_path: str):
        with open(dest_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(table)


class SolrResultTransformer:
    def __init__(self, table: Table, format: TargetExportFormat, transformed_result_path: str):
        self._table = table
        self._format = format
        self._transformed_result_path = transformed_result_path

    def _rename_table_columns(self):
        """Renames the solr columns to the public names as specified in the JSON schema"""

        # TODO: fill out the rest of these fields
        renaming_map = {
            SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME: METADATA_PLACE_NAME
        }
        petl.rename(self._table, renaming_map, strict=False)

    def transform(self):
        self._rename_table_columns()
        if self._format == TargetExportFormat.CSV:
            CSVExportTransformer.transform(self._table, self._transformed_result_path)
        else:
            raise ExportTransformException(f"Unsupported export format: {self._format}")
