import logging

import click
import click_config_file
from click import Context
from tabulate import tabulate

import isb_lib.core
from isb_lib.data_import import csv_import
from frictionless import validate_package

from isb_lib.data_import.csv_import import things_from_isamples_package
from isb_web import config
from isb_web.sqlmodel_database import SQLModelDAO


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
    isb_lib.core.things_main(ctx, config.Settings().database_url, config.Settings().solr_url, verbosity, heart_rate)


@main.command("validate")
@click.option(
    "-f",
    "--file",
    type=str,
    help="Path to the CSV file containing the samples to load",
    required=True
)
def validate_isamples_package(file: str):
    package = csv_import.create_isamples_package(file)
    report = package.validate()
    if report.valid:
        print("Validation successful.")
    else:
        print_report_errors(report)


def print_report_errors(report):
    errors = report.flatten(['code', 'message'])
    print(tabulate(errors, headers=['code', 'message']))


@main.command("load")
@click.option(
    "-f",
    "--file",
    type=str,
    help="Path to the CSV file containing the samples to load",
    required=True
)
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=-1,
    help="Maximum records to load, -1 for all",
)
@click.pass_context
def load_records(ctx: Context, file: str, max_records: int):
    package = csv_import.create_isamples_package(file)
    report = package.validate()
    if not report.valid:
        print_report_errors(report)
        return
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    things = things_from_isamples_package(session, package, max_records)
    logging.info(f"Successfully imported {len(things)} things.")


def _validate_resolved_content(thing: isb_lib.models.thing.Thing) -> dict:
    return isb_lib.core.validate_resolved_content(config.Settings().authority_id, thing)


def reparse_as_core_record(thing: isb_lib.models.thing.Thing) -> list[dict]:
    # No transformation necessary since we already import the data in our solr format
    resolved_content = _validate_resolved_content(thing)
    return [resolved_content]


@main.command("populate_isb_core_solr")
@click.pass_context
def populate_isb_core_solr(ctx):
    db_url = ctx.obj["db_url"]
    solr_url = ctx.obj["solr_url"]
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=config.Settings().authority_id,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url=solr_url
    )
    allkeys = solr_importer.run_solr_import(
        reparse_as_core_record
    )
    logging.info(f"Total keys= {len(allkeys)}")


if __name__ == "__main__":
    main()
