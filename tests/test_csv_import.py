import pytest
from frictionless import validate, Resource

from isb_lib.data_import import csv_import

CSV_items = [
    "./test_data/isb_core_csv_documents/simple_isamples.csv",
]


@pytest.mark.parametrize("csv_file_path", CSV_items)
def test_load_csv(csv_file_path: str):
    records = csv_import.create_isamples_package(csv_file_path)
    report = validate(records.to_dict(), type="package")
    assert report.valid
    first_resource: Resource = records.resources[0]
    for row in first_resource:
        if row is None:
            break
        print(f"row is {row}")
        print(f"row errors are {row.errors}")
        assert row.errors is None or len(row.errors) == 0


@pytest.mark.parametrize("csv_file_path", CSV_items)
def test_unflatten_csv_row(csv_file_path: str):
    records = csv_import.create_isamples_package(csv_file_path)
    first_resource: Resource = records.resources[0]
    for row in first_resource:
        unflattened_row = csv_import.unflatten_csv_row(row)
        assert unflattened_row is not None


