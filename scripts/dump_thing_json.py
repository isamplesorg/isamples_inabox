import json
import os.path

import click

import isb_lib.core
from isb_web.sqlmodel_database import SQLModelDAO, paged_things_with_ids


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
@click.option(
    "-a",
    "--authority",
    default="SMITHSONIAN",
    help="Which authority to use when selecting and dumping the resolved_content",
)
@click.option(
    "-c",
    "--count",
    default=10000,
    help="The number of things to dump",
)
@click.option(
    "-p",
    "--path",
    default=None,
    help="The path to write out the JSON files",
)
@click.pass_context
def main(ctx, db_url: str, verbosity: str, authority: str, count: int, path: str):
    """Program to dump the specified number of things to the specified path"""
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    if not os.path.exists(path):
        os.mkdir(path)
        print(f"Created directory at {path}")
    session = SQLModelDAO((db_url)).get_session()
    things = paged_things_with_ids(session, authority, 200, count)
    for thing in things:
        thing_id = thing.id
        if thing_id is not None:
            id = thing_id.replace("http://opencontext.org/subjects/", "")
            dest_path = os.path.join(path, f"{id}.json")
            with open(dest_path, "w") as json_file:
                json.dump(thing.resolved_content, json_file)


if __name__ == "__main__":
    main()
