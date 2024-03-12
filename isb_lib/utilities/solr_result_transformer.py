from abc import ABC
from enum import StrEnum

import petl
from petl import Table

from isamples_metadata.metadata_constants import METADATA_PLACE_NAME, METADATA_AUTHORIZED_BY, METADATA_COMPLIES_WITH, \
    METADATA_LONGITUDE, METADATA_LATITUDE, METADATA_RELATED_RESOURCE, \
    METADATA_CURATION_LOCATION, METADATA_ACCESS_CONSTRAINTS, METADATA_CURATION, METADATA_SAMPLING_PURPOSE, \
    METADATA_REGISTRANT, METADATA_SAMPLE_LOCATION, METADATA_ELEVATION, METADATA_SAMPLING_SITE, METADATA_IDENTIFIER, \
    METADATA_RESULT_TIME, METADATA_HAS_FEATURE_OF_INTEREST, METADATA_DESCRIPTION, METADATA_INFORMAL_CLASSIFICATION, \
    METADATA_KEYWORDS, METADATA_HAS_SPECIMEN_CATEGORY, METADATA_HAS_MATERIAL_CATEGORY, METADATA_HAS_CONTEXT_CATEGORY, \
    METADATA_LABEL
from isamples_metadata.solr_field_constants import SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, SOLR_AUTHORIZED_BY, \
    SOLR_COMPLIES_WITH, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, SOLR_RELATED_RESOURCE_ISB_CORE_ID, SOLR_CURATION_RESPONSIBILITY, \
    SOLR_CURATION_LOCATION, SOLR_CURATION_ACCESS_CONSTRAINTS, SOLR_CURATION_DESCRIPTION, SOLR_CURATION_LABEL, \
    SOLR_SAMPLING_PURPOSE, SOLR_REGISTRANT, SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_ID, \
    SOLR_PRODUCED_BY_RESULT_TIME, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, \
    SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_LABEL, SOLR_PRODUCED_BY_ISB_CORE_ID, SOLR_INFORMAL_CLASSIFICATION, \
    SOLR_KEYWORDS, SOLR_HAS_SPECIMEN_CATEGORY, SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_CONTEXT_CATEGORY, SOLR_DESCRIPTION, \
    SOLR_LABEL, SOLR_SOURCE


class ExportTransformException(Exception):
    """Exception subclass for when an error occurs during export transform"""


class TargetExportFormat(StrEnum):
    """Valid target export formats"""
    CSV = "CSV"
    JSON = "JSON"
    JSONL = "JSONL"

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
    def transform(table: Table, dest_path_no_extension: str, append: bool) -> str:
        """Transform solr results into a target export format"""
        pass


class CSVExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(table: Table, dest_path_no_extension: str, append: bool) -> str:
        dest_path = f"{dest_path_no_extension}.csv"
        if append:
            petl.io.csv.appendcsv(table, dest_path)
        else:
            petl.io.csv.tocsv(table, dest_path)
        return dest_path


class JSONExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(table: Table, dest_path_no_extension: str, append: bool, is_lines: bool) -> str:
        if append:
            raise ValueError("JSON Export doesn't support appending")
        extension = "jsonl" if is_lines else "json"
        dest_path = f"{dest_path_no_extension}.{extension}"
        petl.tojson(table, dest_path, sort_keys=True, lines=is_lines)
        return dest_path


class SolrResultTransformer:
    def __init__(self, table: Table, format: TargetExportFormat, result_uuid: str, append: bool):
        self._table = table
        self._format = format
        self._result_uuid = result_uuid
        self._append = append

    def _rename_table_columns(self):
        """Renames the solr columns to the public names as specified in the JSON schema"""

        renaming_map = {
            SOLR_ID: METADATA_IDENTIFIER,
            SOLR_AUTHORIZED_BY: METADATA_AUTHORIZED_BY,
            SOLR_COMPLIES_WITH: METADATA_COMPLIES_WITH,
            SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE: METADATA_LONGITUDE,
            SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE: METADATA_LATITUDE,
            SOLR_RELATED_RESOURCE_ISB_CORE_ID: METADATA_RELATED_RESOURCE,
            SOLR_CURATION_RESPONSIBILITY: "curation_responsibility",  # Note that in the metadata this is just "responsibility", but this is a flat export format so we can't use that key by itself
            SOLR_CURATION_LOCATION: METADATA_CURATION_LOCATION,
            SOLR_CURATION_ACCESS_CONSTRAINTS: METADATA_ACCESS_CONSTRAINTS,
            SOLR_CURATION_DESCRIPTION: METADATA_CURATION,
            SOLR_CURATION_LABEL: "curation_label",  # Note that in the metadata this is just "label", but this is a flat export format so we can't use that key by itself
            SOLR_SAMPLING_PURPOSE: METADATA_SAMPLING_PURPOSE,
            SOLR_REGISTRANT: METADATA_REGISTRANT,
            SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME: METADATA_PLACE_NAME,
            SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS: METADATA_ELEVATION,
            SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL: METADATA_SAMPLE_LOCATION,
            SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION: METADATA_SAMPLING_SITE,
            SOLR_PRODUCED_BY_RESULT_TIME: METADATA_RESULT_TIME,
            SOLR_PRODUCED_BY_RESPONSIBILITY: "produced_by_responsibility",  # Note that in the metadata this is just "responsibility", but this is a flat export format so we can't use that key by itself
            SOLR_PRODUCED_BY_FEATURE_OF_INTEREST: METADATA_HAS_FEATURE_OF_INTEREST,
            SOLR_PRODUCED_BY_DESCRIPTION: "produced_by_description",  # Note that in the metadata this is just "description", but this is a flat export format so we can't use that key by itself
            SOLR_PRODUCED_BY_LABEL: "produced_by_label",  # Note that in the metadata this is just "label", but this is a flat export format so we can't use that key by itself
            SOLR_PRODUCED_BY_ISB_CORE_ID: "produced_by_id",  # Note that in the metadata this is just "produced_by", but this is a flat export format so we can't use that key by itself
            SOLR_INFORMAL_CLASSIFICATION: METADATA_INFORMAL_CLASSIFICATION,
            SOLR_KEYWORDS: METADATA_KEYWORDS,
            SOLR_HAS_SPECIMEN_CATEGORY: METADATA_HAS_SPECIMEN_CATEGORY,
            SOLR_HAS_MATERIAL_CATEGORY: METADATA_HAS_MATERIAL_CATEGORY,
            SOLR_HAS_CONTEXT_CATEGORY: METADATA_HAS_CONTEXT_CATEGORY,
            SOLR_DESCRIPTION: METADATA_DESCRIPTION,
            SOLR_LABEL: METADATA_LABEL,
            SOLR_SOURCE: "source_collection",  # this isn't present in the exported metadata
        }

        self._table = petl.transform.headers.rename(self._table, renaming_map, strict=False)
        self._table = petl.rename(self._table, renaming_map, strict=False)

    def transform(self) -> str:
        """Transforms the table to the destination format.  Return value is the path the output file was written to."""
        self._rename_table_columns()
        if self._format == TargetExportFormat.CSV:
            return CSVExportTransformer.transform(self._table, self._result_uuid, self._append)
        elif self._format == TargetExportFormat.JSON or self._format == TargetExportFormat.JSONL:
            return JSONExportTransformer.transform(self._table, self._result_uuid, self._append, self._format == TargetExportFormat.JSONL)
        else:
            raise ExportTransformException(f"Unsupported export format: {self._format}")
