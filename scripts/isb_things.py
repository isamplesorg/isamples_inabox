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


if __name__ == "__main__":
    main()
