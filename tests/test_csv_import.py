import pytest
from frictionless import validate, Resource
import json

from isb_lib.data_import import csv_import

CSV_items = [
    (
        "./test_data/isb_core_csv_documents/simple_isamples.csv",
        "./test_data/isb_core_csv_documents/simple_isamples.json"
    )
]


@pytest.mark.parametrize("csv_file_path,json_file_path", CSV_items)
def test_load_csv(csv_file_path: str, json_file_path: str):
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


def test_serialized_json_dict(dict1: dict, dict2: dict):
    """Simple method that will try to serialize non-str to str in order to compare against a serialized JSON dict"""
    for k, v in dict1.items():
        if type(v) is dict:
            test_serialized_json_dict(v, dict2[k])
        else:
            assert str(v) == str(dict2[k])


@pytest.mark.parametrize("csv_file_path,json_file_path", CSV_items)
def test_unflatten_csv_row(csv_file_path: str, json_file_path: str):
    records = csv_import.create_isamples_package(csv_file_path)
    with open(json_file_path) as json_file:
        json_str = json_file.read()
        json_contents = json.loads(json_str)
    first_resource: Resource = records.resources[0]
    for row in first_resource:
        unflattened_row = csv_import.unflatten_csv_row(row)
        assert unflattened_row is not None
        test_serialized_json_dict(unflattened_row, json_contents)


