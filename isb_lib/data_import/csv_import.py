import csv
from typing import Any


def import_isamples_csv(file_path: str) -> list[dict[str, Any]]:
    """
    Opens the specified file and return it as a list of iSamples Core record dictionaries
    Args:
        file_path: The path to the file to open

    Returns: A list of dictionaries containing the records
    """
    with open(file_path, newline="") as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
        rows = []
        for row in csvreader:
            rows.append(row)
        return rows