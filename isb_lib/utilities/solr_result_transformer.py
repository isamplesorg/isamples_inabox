import json
from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum

import petl
from petl import Table

from isamples_metadata.metadata_constants import METADATA_PLACE_NAME, METADATA_AUTHORIZED_BY, METADATA_COMPLIES_WITH, \
    METADATA_LONGITUDE, METADATA_LATITUDE, METADATA_RELATED_RESOURCE, \
    METADATA_CURATION_LOCATION, METADATA_ACCESS_CONSTRAINTS, METADATA_CURATION, METADATA_SAMPLING_PURPOSE, \
    METADATA_REGISTRANT, METADATA_SAMPLE_LOCATION, METADATA_ELEVATION, METADATA_SAMPLING_SITE, \
    METADATA_RESULT_TIME, METADATA_HAS_FEATURE_OF_INTEREST, METADATA_DESCRIPTION, METADATA_INFORMAL_CLASSIFICATION, \
    METADATA_KEYWORDS, METADATA_HAS_SPECIMEN_CATEGORY, METADATA_HAS_MATERIAL_CATEGORY, METADATA_HAS_CONTEXT_CATEGORY, \
    METADATA_LABEL, METADATA_SAMPLE_IDENTIFIER, METADATA_AT_ID, METADATA_RESPONSIBILITY, METADATA_PRODUCED_BY, \
    METADATA_NAME
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


class TargetExportFormat(Enum):
    """Valid target export formats"""
    CSV = "CSV"
    JSON = "JSON"

    # overridden to allow for case insensitivity in query parameter formatting
    @classmethod
    def _missing_(cls, value):
        value = value.upper()
        for member in cls:
            if member.value.upper() == value:
                return member
        return None


class AbstractExportTransformer(ABC):
    @staticmethod
    @abstractmethod
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
    def transform(table: Table, dest_path_no_extension: str, append: bool) -> str:
        if append:
            raise ValueError("JSON Export doesn't support appending")
        extension = "jsonl"
        dest_path = f"{dest_path_no_extension}.{extension}"
        with open(dest_path, "w") as file:
            for row in petl.util.base.dicts(table):
                json.dump(row, file)
                file.write("\n")
        return dest_path


class SolrResultTransformer:
    def __init__(self, table: Table, format: TargetExportFormat, result_uuid: str, append: bool):
        self._table = table
        self._format = format
        self._result_uuid = result_uuid
        self._append = append

    def _add_to_dict(self, target_dict: dict, target_key: str, source_dict: dict, source_key: str, default_value: str = ""):
        source_value = source_dict.get(source_key, default_value)
        if source_value is not None:
            target_dict[target_key] = source_value

    def _curation_dict(self, rec: dict) -> dict:
        curation_dict: dict = {}
        self._add_to_dict(curation_dict, METADATA_LABEL, rec, SOLR_CURATION_LABEL)
        self._add_to_dict(curation_dict, METADATA_DESCRIPTION, rec, SOLR_CURATION_DESCRIPTION)
        self._add_to_dict(curation_dict, METADATA_ACCESS_CONSTRAINTS, rec, SOLR_CURATION_ACCESS_CONSTRAINTS)
        self._add_to_dict(curation_dict, METADATA_CURATION_LOCATION, rec, SOLR_CURATION_LOCATION)
        self._add_to_dict(curation_dict, METADATA_RESPONSIBILITY, rec, SOLR_CURATION_RESPONSIBILITY)
        return curation_dict

    def _produced_by_dict(self, rec: dict) -> dict:
        produced_by_dict: dict = {}
        self._add_to_dict(produced_by_dict, METADATA_AT_ID, rec, SOLR_PRODUCED_BY_ISB_CORE_ID)
        self._add_to_dict(produced_by_dict, METADATA_LABEL, rec, SOLR_PRODUCED_BY_LABEL)
        self._add_to_dict(produced_by_dict, METADATA_RESPONSIBILITY, rec, SOLR_PRODUCED_BY_RESPONSIBILITY)
        self._add_to_dict(produced_by_dict, METADATA_DESCRIPTION, rec, SOLR_PRODUCED_BY_DESCRIPTION)
        self._add_to_dict(produced_by_dict, METADATA_RESULT_TIME, rec, SOLR_PRODUCED_BY_RESULT_TIME)
        self._add_to_dict(produced_by_dict, METADATA_HAS_FEATURE_OF_INTEREST, rec, SOLR_PRODUCED_BY_FEATURE_OF_INTEREST)
        sampling_site_dict: dict = {}
        produced_by_dict[METADATA_SAMPLING_SITE] = sampling_site_dict
        self._add_to_dict(sampling_site_dict, METADATA_DESCRIPTION, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION)
        self._add_to_dict(sampling_site_dict, METADATA_LABEL, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL)
        self._add_to_dict(sampling_site_dict, METADATA_PLACE_NAME, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME)
        sample_location_dict: dict = {}
        sampling_site_dict[METADATA_SAMPLE_LOCATION] = sample_location_dict
        self._add_to_dict(sampling_site_dict, METADATA_ELEVATION, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS)
        self._add_to_dict(sampling_site_dict, METADATA_LATITUDE, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE)
        self._add_to_dict(sampling_site_dict, METADATA_LONGITUDE, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE)
        return produced_by_dict

    def _registrant_dict(self, rec: dict) -> dict:
        return {METADATA_NAME: rec[SOLR_REGISTRANT]}

    def _rename_table_columns(self):
        """Renames the solr columns to the public names as specified in the JSON schema"""

        mappings = OrderedDict()
        mappings[METADATA_SAMPLE_IDENTIFIER] = SOLR_ID
        mappings[METADATA_LABEL] = SOLR_LABEL
        mappings[METADATA_DESCRIPTION] = SOLR_DESCRIPTION
        mappings["source_collection"] = SOLR_SOURCE  # this isn't present in the exported metadata
        mappings[METADATA_HAS_SPECIMEN_CATEGORY] = SOLR_HAS_SPECIMEN_CATEGORY
        mappings[METADATA_HAS_MATERIAL_CATEGORY] = SOLR_HAS_MATERIAL_CATEGORY
        mappings[METADATA_HAS_CONTEXT_CATEGORY] = SOLR_HAS_CONTEXT_CATEGORY
        mappings[METADATA_INFORMAL_CLASSIFICATION] = SOLR_INFORMAL_CLASSIFICATION
        mappings[METADATA_KEYWORDS] = SOLR_KEYWORDS
        mappings[METADATA_PRODUCED_BY] = self._produced_by_dict
        mappings[METADATA_REGISTRANT] = self._registrant_dict
        mappings[METADATA_SAMPLING_PURPOSE] = SOLR_SAMPLING_PURPOSE
        mappings[METADATA_CURATION] = self._curation_dict
        mappings[METADATA_RELATED_RESOURCE] = SOLR_RELATED_RESOURCE_ISB_CORE_ID
        mappings[METADATA_AUTHORIZED_BY] = SOLR_AUTHORIZED_BY
        mappings[METADATA_COMPLIES_WITH] = SOLR_COMPLIES_WITH
        self._table = petl.fieldmap(self._table, mappings)

    def transform(self) -> str:
        """Transforms the table to the destination format.  Return value is the path the output file was written to."""
        self._rename_table_columns()
        if self._format == TargetExportFormat.CSV:
            return CSVExportTransformer.transform(self._table, self._result_uuid, self._append)
        elif self._format == TargetExportFormat.JSON or self._format == TargetExportFormat.JSONL:
            return JSONExportTransformer.transform(self._table, self._result_uuid, self._append)
        else:
            raise ExportTransformException(f"Unsupported export format: {self._format}")
