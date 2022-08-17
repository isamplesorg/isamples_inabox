from typing import Generator

import pytest
from frictionless import Resource, Row

from isb_lib.data_import import csv_import

# CSV_items = [
#     "./test_data/isb_core_csv_documents/simple_isamples.csv",
# ]


# @pytest.mark.parametrize("csv_file_path", CSV_items)
# def test_load_csv(csv_file_path: str):
def test_load_csv():
    # records = csv_import.import_isamples_csv(csv_file_path)
    records = csv_import.import_isamples_csv("")
    print(f"values are {records.values()}")
    print(f"items are {records.items()}")
    print(f"resources are {records.resources}")
    first_resource: Resource = records.resources[0]
    for row in first_resource:
        if row is None:
            break
        print(f"row is {row}")
        print(f"row errors are {row.errors}")

