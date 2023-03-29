import logging
from typing import Optional

import click
import requests
from sqlmodel import Session

import isb_lib
import isb_lib.core
import isb_web.config
import isb_lib.geome_adapter
from isamples_metadata import GEOMETransformer
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO, taxonomy_name_to_kingdom_map


@click.command()
@click.pass_context
def main(ctx):
    db_url = isb_web.config.Settings().database_url
    solr_url = isb_web.config.Settings().solr_url
    isb_lib.core.things_main(ctx, None, solr_url)
    session = SQLModelDAO(db_url).get_session()
    add_kingdom_data(session, solr_url)


def add_kingdom_data(session: Session, solr_url: str):
    map = taxonomy_name_to_kingdom_map(session)
    batch_size = 10000
    thing_iterator = isb_lib.core.ThingRecordIterator(
        session,
        authority_id=isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID,
        page_size=batch_size,
        offset=0
    )
    total_things = 0
    num_with_resolved_kingdom = 0
    uniqued_unknown_names = set()
    # Gather the kingdom values by sample id
    sample_id_to_kingdom = {}
    for thing in thing_iterator.yieldRecordsByPage():
        transformer = GEOMETransformer.GEOMETransformer(source_record=thing.resolved_content)
        total_things += 1
        if total_things % 1000 == 0:
            logging.info(f"Visited {total_things} things, current percentage of things with identified kingdom: {num_with_resolved_kingdom / total_things}")
        ranks = ["kingdom", "phylum", "genus"]
        checked_ranks = []
        record = thing.resolved_content["record"]
        for rank in ranks:
            value = record.get(rank)
            if value is not None and value != "unidentified":
                checked_ranks.append(value)
                # Check to see if we have a hit in our name cache
                resolved_kingdom = map.get(value)
                if resolved_kingdom is not None:
                    num_with_resolved_kingdom += 1
                    break
        if resolved_kingdom is None:
            print(f"couldnt find kingdom for {checked_ranks}")
            for rank in checked_ranks:
                uniqued_unknown_names.add(rank)
        else:
            sample_id_to_kingdom[transformer.sample_identifier_string()] = resolved_kingdom
            for child_transformer in transformer.child_transformers:
                sample_id_to_kingdom[child_transformer.sample_identifier_string()] = resolved_kingdom
    logging.info(f"missing values for {uniqued_unknown_names}")

    _do_solr_import(batch_size, sample_id_to_kingdom, solr_url)


def _do_solr_import(batch_size, sample_id_to_kingdom, solr_url):
    total_records = 0
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, "source:GEOME", batch_size, 0, "id asc")
    for record in iterator:
        mutated_record = mutate_record(record, sample_id_to_kingdom)
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


def mutate_record(record: dict, sample_id_to_kingdom: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    kingdom_for_sample = sample_id_to_kingdom.get(record["id"])
    if kingdom_for_sample is not None:
        record_copy = record.copy()
        record_copy["hasContextCategory"] = [kingdom_for_sample]
        return record_copy
    else:
        return None


"""
Adds context categories for GEOME solr records
"""
if __name__ == "__main__":
    main()
