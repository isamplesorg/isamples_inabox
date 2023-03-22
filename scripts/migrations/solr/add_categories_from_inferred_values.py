import logging
from typing import Optional

import click
import requests
# Note that pandas isn't part of the poetry-managed dependencies because the main box doesn't actually require it.
# Since this run as a one-off manual process, just make sure the venv you run it in has pandas installed
import pandas

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isamples_metadata.Transformer import Transformer
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator


@click.command()
@click.option(
    "-f", "--file", default=None, help="Path to the tsv file with the category values"
)
@click.option(
    "-a",
    "--authority",
    default=None,
    help="The authority to import data for",
)
@click.pass_context
def main(ctx, file: str, authority: str):
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, None, solr_url)
    values = parse_values_to_dict(file, authority)
    add_category_values(solr_url, authority, values)


def parse_values_to_dict(file_path: str, authority: str) -> dict:
    values = pandas.read_csv(file_path, sep="\t")
    all_rows = {}
    num_rows = len(values)
    for row in range(num_rows):
        row = values.iloc[row]
        dict_for_row = {}
        material_categories = handle_string_column(row, "hasMaterialCategory")
        dict_for_row["hasMaterialCategory"] = material_categories
        confidences = handle_confidence_column(material_categories, "hasMaterialCategoryConfidence", row)
        dict_for_row["hasMaterialCategoryConfidence"] = confidences
        row_id = row["id"]
        if authority == "OPENCONTEXT":
            row_id = row_id.removeprefix(Transformer.N2T_NO_HTTPS_PREFIX)
            specimen_categories = handle_string_column(row, "hasSpecimenCategory")
            dict_for_row["hasSpecimenCategory"] = specimen_categories
            confidences = handle_confidence_column(specimen_categories, "hasSpecimenCategoryConfidence", row)
            dict_for_row["hasSpecimenCategoryConfidence"] = confidences
        all_rows[row_id] = dict_for_row
    return all_rows


def handle_string_column(row, key):
    return row[key].split(",")


def handle_confidence_column(material_categories, key, row):
    confidences = row[key]
    if type(confidences) is str:
        confidences = confidences.split(",")
        confidences = [float(confidence) for confidence in confidences]
    else:
        confidences = [Transformer.RULE_BASED_CONFIDENCE for i in range(len(material_categories))]
    return confidences


def add_category_values(solr_url: str, authority: str, values: dict):
    total_records = 0
    batch_size = 50000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, f"source:{authority}", batch_size, 0, "id asc")
    for record in iterator:
        mutated_record = mutate_record(record, values)
        if mutated_record is not None:
            current_mutated_batch.append(mutated_record)
        if len(current_mutated_batch) == batch_size:
            save_mutated_batch(current_mutated_batch, rsession, solr_url)
            current_mutated_batch = []
        total_records += 1
    if len(current_mutated_batch) > 0:
        # handle the remainder
        save_mutated_batch(current_mutated_batch, rsession, solr_url)
    logging.info(f"Finished iterating, visited {total_records} records")


def save_mutated_batch(current_mutated_batch, rsession, solr_url):
    logging.info(f"Going to save {len(current_mutated_batch)} records")
    isb_lib.core.solrAddRecords(rsession, current_mutated_batch, solr_url)
    isb_lib.core.solrCommit(rsession, solr_url)
    logging.info(f"Just saved {len(current_mutated_batch)} records")


def mutate_record(record: dict, values: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    values_for_record = values.get(record["id"])
    if values_for_record is None:
        return None
    record_copy = record.copy()
    for k, v in values_for_record.items():
        record_copy[k] = v
    return record_copy


"""
Integrates output from machine_learned script into Solr index
"""
if __name__ == "__main__":
    main()
