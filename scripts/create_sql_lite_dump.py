import click
import click_config_file
import isb_lib.core
from isb_lib.models.isb_core_record import ISBCoreRecord
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
import requests
from sqlmodel import SQLModel, create_engine, Session, select


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="The SQLite database file URL for storage.  Doesn't need to exist beforehand."
)
@click.option(
    "-s", "--solr_url", default=None, help="Solr index URL"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnostics on 9999"
)
@click.option(
    "-a",
    "--authority",
    default=None,
    help="Which authority to use when selecting and dumping the resolved_content",
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, solr_url, verbosity, heart_rate, authority):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity, heart_rate)
    engine = create_engine(ctx.obj["db_url"], echo=False)
    SQLModel.metadata.drop_all(engine, tables=[ISBCoreRecord.__table__])
    SQLModel.metadata.create_all(engine, tables=[ISBCoreRecord.__table__])
    session = Session(engine)
    total_records = 0
    batch_size = 1000
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(rsession, authority, batch_size, 0, "id asc")
    for record in iterator:
        # These bookkeeping fields in solr shouldn't end up in the dump
        record.pop("_version_", None)
        record.pop("producedBy_samplingSite_location_bb__minY", None)
        record.pop("producedBy_samplingSite_location_bb__minX", None)
        record.pop("producedBy_samplingSite_location_bb__maxY", None)
        record.pop("producedBy_samplingSite_location_bb__maxX", None)
        new_record = ISBCoreRecord()
        new_record.id = record["id"]
        session.add(new_record)
        session.commit()


"""
Dumps the iSamples in a Box Solr Core records to a SQLite file
"""
if __name__ == "__main__":
    main()
