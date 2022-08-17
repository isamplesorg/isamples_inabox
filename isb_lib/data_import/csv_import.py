import csv
from typing import Any

from frictionless import describe, Package


def import_isamples_csv(file_path: str) -> Package:
    """
    Opens the specified file and return it as a list of iSamples Core record dictionaries
    Args:
        file_path: The path to the file to open

    Returns: A list of dictionaries containing the records
    """
    # TODO sort out paths
    package = describe("/Users/mandeld/iSamples/isamples_docker/isb/isamples_inabox/isb_lib/data_import/datapackage.json", type="package")
    print(package)
    return package
