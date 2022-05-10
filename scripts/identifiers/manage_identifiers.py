import click
import click_config_file

import isb_lib.core
import isb_lib.datacite as datacite
import requests
import logging

from isb_web.sqlmodel_database import SQLModelDAO, save_draft_thing_with_id


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url):
    isb_lib.core.things_main(ctx, db_url, None)


@main.command("create_draft")
@click.option(
    "--num_identifiers",
    type=int,
    default=1,
    help="Number of draft identifiers to create.",
)
@click.option(
    "--prefix",
    type=str,
    default=None,
    help="The datacite prefix to use when creating identifiers.",
)
@click.option(
    "--username",
    type=str,
    default=None,
    help="The datacite username to use when creating identifiers.",
)
@click.password_option(hide_input=True)
@click.pass_context
def create_draft_identifiers(ctx, num_identifiers, prefix, username, password):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    for i in range(num_identifiers):
        draft_id = datacite.create_draft_doi(
            requests.session(), prefix, username, password
        )
        if draft_id is not None:
            # user the DOI prefix since we creating DOIs with datacite
            draft_id = f"doi:{draft_id}"
            logging.info("Successfully created draft DOI %s", draft_id)
            draft_thing = save_draft_thing_with_id(session, draft_id)
            logging.info("Successfully saved draft thing with id %s", draft_thing.id)


if __name__ == "__main__":
    main()
