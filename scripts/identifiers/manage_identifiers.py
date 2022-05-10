import click
import click_config_file

import isb_lib.core
import isb_lib.datacite as datacite
import requests


@click.group()
@click.option("-d", "--db_url", default=None, help="SQLAlchemy database URL for storage")
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url):
    isb_lib.core.things_main(ctx, db_url, None)


@main.command("create_draft")
@click.option("--num_identifiers", type=int, default=1, help="Number of draft identifiers to create.")
@click.option("--prefix", type=str, default=None, help="The datacite prefix to use when creating identifiers.")
@click.option("--username", type=str, default=None, help="The datacite username to use when creating identifiers.")
@click.password_option(hide_input=True)
def create_draft_identifiers(num_identifiers, prefix, username, password):
    for i in range(num_identifiers):
        datacite.create_draft_doi(requests.session(), prefix, username, password)


if __name__ == "__main__":
    main()
