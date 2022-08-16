import pytest
from isb_lib.data_import import csv_import

CSV_items = [
    "./test_data/isb_core_csv_documents/simple_isamples.csv",
]


@pytest.mark.parametrize("csv_file_path", CSV_items)
def test_load_csv(csv_file_path: str):
    records = csv_import.import_isamples_csv(csv_file_path)
    assert 1 == len(records)