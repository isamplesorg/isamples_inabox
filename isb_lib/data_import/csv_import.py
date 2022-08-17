from frictionless import Package, Resource
from pathlib import Path
import os.path
import json

p = Path(__file__)
SCHEMA_JSON = {}
schema_json_path = os.path.join(p.parent, "isamples_simple_schema.json")
with open(schema_json_path) as schema_json_file:
    SCHEMA_JSON = json.load(schema_json_file)


def import_isamples_csv(file_path: str) -> Package:
    """
    Opens the specified file and return it as a list of iSamples Core record dictionaries
    Args:
        file_path: The path to the file to open

    Returns: A list of dictionaries containing the records
    """
    data_resource = Resource(source=file_path, format="csv", mediatype="text/csv", schema=SCHEMA_JSON)
    package = Package(resources=[data_resource], name="isamples", title="isamples", id="isamples")
    return package
