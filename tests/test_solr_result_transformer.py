import json
import os.path
import uuid

import petl
import pytest

from isb_lib.utilities.solr_result_transformer import SolrResultTransformer, TargetExportFormat

SOLR_items = [
    "./test_data/solr_results/test_solr_results.json",
]


@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_solr_result_transformer(solr_file_path: str):
    new_uuid = uuid.uuid4()
    with open(solr_file_path, "r") as file:
        solr_result_dict: dict = json.load(file)
        result_set: dict = solr_result_dict.get("result-set", {})
        if result_set is not None:
            docs: list[dict] = result_set.get("docs", {})
            table = petl.fromdicts(docs)
            dest_path = f"./test_data/solr_results/solr_results_{new_uuid}.csv"
            solr_result_transformer = SolrResultTransformer(table, TargetExportFormat.CSV, dest_path)
            solr_result_transformer.transform()
            assert os.path.exists(dest_path)
