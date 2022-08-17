import logging
import click
import click_config_file
from click import Context
from tabulate import tabulate

import isb_lib.core
from isb_lib.data_import import csv_import
from frictionless import validate


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option("-s", "--solr_url", default=None, help="Solr index URL")
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
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, solr_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity, heart_rate)


@main.command("validate")
@click.option(
    "-f",
    "--file",
    type=str,
    help="Path to the CSV file containing the samples to load",
    required=True
)
def validate_package(file: str):
    package = csv_import.create_isamples_package(file)
    report = validate(package.to_dict(), type="package")
    if report.valid:
        logging.info("Validation successful.")
    else:
        errors = report.flatten(['code', 'message'])
        print(tabulate(errors, headers=['code', 'message']))


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
    type=str,
    default=None,
    help="Path to the CSV file containing the samples to load"
)
@click.pass_context
def load_records(ctx: Context, max_records: int, file_path: str):
    package = csv_import.create_isamples_package(file_path)
    report = validate_package(package.to_dict(), type="package")
    if not report.valid:
        print(report.to_summary())
    # if package.validate():
    #     pac
    # if len(package.metadata_errors) > 0:
    #     print("Error")
    # session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    # max_created = sqlmodel_database.last_time_thing_created(
    #     session, isb_lib.opencontext_adapter.OpenContextItem.AUTHORITY_ID
    # )
    # L.info("loadRecords: %s", str(session))
    # # ctx.obj["db_url"] = db_url
    # load_open_context_entries(session, max_records, max_created)


if __name__ == "__main__":
    main()