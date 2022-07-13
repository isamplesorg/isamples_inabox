import click
import click_config_file
from isamples_metadata import SESARTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO


@click.command()
@click.option(
    "-d",
    "--db_url",
    default=None,
    help="Postgres database URL",
)
@click.option("-s", "--solr_url", default=None, help="Solr index URL")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
# You can specify the filename by doing --config <file> as the cmdline option
@click_config_file.configuration_option()
@click.pass_context
def main(ctx, db_url: str, solr_url: str, max_records: int, verbosity: str):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity)
    db_session = SQLModelDAO(db_url).get_session()
    thing_iterator = ThingRecordIterator(
        db_session, authority_id="SESAR", page_size=max_records
    )
    for thing in thing_iterator.yieldRecordsByPage():
        print(f"thing is {thing.id}")
        transformed = SESARTransformer.SESARTransformer(
            thing.resolved_content
        ).transform()
        print(
            f"context: {transformed['hasContextCategory']} material: {transformed['hasMaterialCategory']} "
            f"specimen: {transformed['hasSpecimenCategory']}"
        )


"""
Exercise new machine driven classification
"""
if __name__ == "__main__":
    main()
