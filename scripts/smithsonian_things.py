import click
import click_config_file
import isb_lib.core
import csv
import json
import isb_lib.smithsonian_adapter
import logging
import sqlalchemy
import igsn_lib.models.thing
import datetime

def _save_record_to_db(session, file_path, record):
    id = record["id"]
    logging.info("got next id from smithsonian %s", id)
    try:
        res = session.query(igsn_lib.models.thing.Thing.id).filter_by(id=id).one()
        logging.info("Already have %s", id)
    except sqlalchemy.orm.exc.NoResultFound:
        logging.debug("Don't have %s", id)
        thing = isb_lib.smithsonian_adapter.load_thing(
            record, datetime.datetime.now(), file_path
        )
        try:
            logging.debug("Going to add thing to session")
            session.add(thing)
            logging.debug("Added thing to session")
            session.commit()
            logging.debug("committed session")
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            logging.error("Item already exists: %s", record)


def load_smithsonian_entries(session, max_count, file_path, start_from=None):
    with open(file_path, newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
        column_headers = []
        i = 0
        for i, current_values in enumerate(csvreader):
            if i == 0:
                column_headers = current_values
                continue
            # Otherwise iterate over the keys and make source JSON
            current_record = {}
            for index, key in enumerate(column_headers):
                current_record[key] = current_values[index]
            _save_record_to_db(session, file_path, current_record)
            if i % 1000 == 0:
                isb_lib.core.getLogger().info("\n\nNum records=%d\n\n", i)


@click.group()
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
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="smithsonian.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, verbosity, heart_rate)


@main.command("load")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.option(
    "-f",
    "--file",
    help="The path to the Darwin Core dump file containing the records to import",
)
@click.pass_context
def load_records(ctx, max_records, file):
    session = isb_lib.core.get_db_session(ctx.obj["db_url"])
    logging.info("loadRecords: %s", str(session))
    load_smithsonian_entries(session, max_records, file, None)


if __name__ == "__main__":
    main()
