import csv
import logging
from abc import ABC
from enum import StrEnum

import petl
from petl import Table

from isamples_metadata.metadata_constants import METADATA_PLACE_NAME
from isamples_metadata.solr_field_constants import SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME
from isb_lib.utilities.enums import _NoValue


class ExportTransformException(Exception):
    """Exception subclass for when an error occurs during export transform"""


class TargetExportFormat(StrEnum):
    """Valid target export formats"""
    CSV = "CSV"
    JSON = "JSON"

    # overridden to allow for case insensitivity in query parameter formatting
    @classmethod
    def _missing_(cls, value):
        value = value.upper()
        for member in cls:
            if member.upper() == value:
                return member
        return None


class AbstractExportTransformer(ABC):
    @staticmethod
    def transform(table: Table, dest_path_no_extension: str):
        """Transform solr results into a target export format"""
        pass


class CSVExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(table: Table, dest_path_no_extension: str):
        with open(f"{dest_path_no_extension}.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(table)


class JSONExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(table: Table, dest_path_no_extension: str):
        petl.tojson(table, f"{dest_path_no_extension}.json", sort_keys=True)


class SolrResultTransformer:
    def __init__(self, table: Table, format: TargetExportFormat, result_uuid: str):
        self._table = table
        self._format = format
        self._result_uuid = result_uuid

    def _rename_table_columns(self):
        """Renames the solr columns to the public names as specified in the JSON schema"""

        # TODO: fill out the rest of these fields
        renaming_map = {
            SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME: METADATA_PLACE_NAME
        }
        petl.rename(self._table, renaming_map, strict=False)

    def transform(self):
        try:
            self._rename_table_columns()
            if self._format == TargetExportFormat.CSV:
                CSVExportTransformer.transform(self._table, self._result_uuid)
            elif self._format == TargetExportFormat.JSON:
                JSONExportTransformer.transform(self._table, self._result_uuid)
            else:
                raise ExportTransformException(f"Unsupported export format: {self._format}")
        except Exception as e:
            logging.error(f"Error transforming for export: {e}")
