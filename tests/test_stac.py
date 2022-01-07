import pytest
import json
import isb_lib.stac
import typing
from jsonschema import validate

SOLR_items = [
    "./test_data/isb_core_solr_documents/ark-21547-Dob2MVZ_HERP_266812.json",
]


@pytest.fixture
def stac_item_schema_json() -> typing.Dict:
    with open("./test_data/stac_schema/item.json") as schema:
        return json.load(schema)


@pytest.fixture
def stac_collection_schema_json() -> typing.Dict:
    with open("./test_data/stac_schema/collection.json") as schema:
        return json.load(schema)


@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_stac_item_valid(solr_file_path, stac_item_schema_json):
    with open(solr_file_path) as source_file:
        solr_document = json.load(source_file)
        stac_item = isb_lib.stac.stac_item_from_solr_dict(
            solr_document, "http://isamples.org/stac/", "http://isamples.org/thing/"
        )
        assert stac_item is not None
        validate(instance=stac_item, schema=stac_item_schema_json)


@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_stac_collection_valid(solr_file_path, stac_collection_schema_json):
    with open(solr_file_path) as source_file:
        solr_document = json.load(source_file)
        stac_collection = isb_lib.stac.stac_collection_from_solr_dicts(
            [solr_document], False, 0, 1, None
        )
        assert stac_collection is not None
        validate(instance=stac_collection, schema=stac_collection_schema_json)
