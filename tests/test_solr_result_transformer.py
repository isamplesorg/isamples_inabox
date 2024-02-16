import os.path
import uuid
import pytest

from isb_lib.utilities.solr_result_transformer import SolrResultTransformer, TargetExportFormat

SOLR_items = [
    "./test_data/solr_results/test_solr_results.json",
]


@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_solr_result_transformer(solr_file_path):
    new_uuid = uuid.uuid4()
    dest_path = f"./test_data/solr_results/solr_results_{new_uuid}.csv"
    solr_result_transformer = SolrResultTransformer(solr_file_path, TargetExportFormat.CSV, dest_path)
    solr_result_transformer.transform()
    assert os.path.exists(dest_path)
