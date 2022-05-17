import json
import pytest

import isb_lib.identifiers.datacite as datacite


def test_datacite_metadata():
    test_file_path = "./test_data/DOIS/thing1.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        prefix = "123456"
        authority = "AUTHORITY"
        doi_metadata = datacite.datacite_metadata_from_core_record(prefix, None, authority, source_record)
        assert doi_metadata is not None
        data_dict = doi_metadata.get("data")
        assert data_dict is not None
        attributes = data_dict.get("attributes")
        assert prefix == attributes.get("prefix")
        assert authority == attributes.get("publisher")


def test_datacite_metadata_missing_label():
    test_file_path = "./test_data/DOIS/missing_label.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        with pytest.raises(ValueError) as e:
            datacite.datacite_metadata_from_core_record("prefix", None, "authority", source_record)
