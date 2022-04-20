import logging

import click
from sqlmodel import Session

import isb_lib
import isb_lib.core
import isb_lib.sesar_adapter
import isb_lib.geome_adapter
import isb_lib.opencontext_adapter
import isb_lib.smithsonian_adapter
from isamples_metadata import SESARTransformer, GEOMETransformer, OpenContextTransformer, SmithsonianTransformer
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import SQLModelDAO

BATCH_SIZE = 50000


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.pass_context
def main(ctx, db_url, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO(db_url).get_session()
    assign_h3(session)


def assign_h3(session: Session):
    authorities = {
        isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID: SESARTransformer.geo_to_h3,
        isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID: GEOMETransformer.geo_to_h3,
        isb_lib.opencontext_adapter.OpenContextItem.AUTHORITY_ID: OpenContextTransformer.geo_to_h3,
        isb_lib.smithsonian_adapter.SmithsonianItem.AUTHORITY_ID: SmithsonianTransformer.geo_to_h3
    }
    for authority, geo_to_h3 in authorities.items():
        logging.info(f"Starting h3 migration for {authority}")
        thing_iterator = isb_lib.core.ThingRecordIterator(
            session,
            authority_id=authority,
            page_size=BATCH_SIZE,
            offset=0,
        )
        i = 0
        update_batch_values = []
        for thing in thing_iterator.yieldRecordsByPage():
            i += 1
            update_batch_values.append(
                {
                    "primary_key": thing.primary_key,
                    "h3": geo_to_h3(thing.resolved_content)
                }
            )
            if i % BATCH_SIZE == 0:
                save_batch(session, update_batch_values)
                i = 0
                update_batch_values = []
        # get the remainder
        save_batch(session, update_batch_values)


def save_batch(session, update_batch_values):
    logging.info(f"About to bulk update h3 value for {len(update_batch_values)} things")
    session.bulk_update_mappings(
        mapper=Thing, mappings=update_batch_values
    )


"""
Assign h3 values for existing Things
"""
if __name__ == "__main__":
    main()
