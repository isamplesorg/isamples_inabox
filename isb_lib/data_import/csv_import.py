import datetime

from frictionless import Package, Resource
from pathlib import Path
import os.path
import json

from sqlmodel import Session

import isb_web.config
from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import save_or_update_thing

p = Path(__file__)
SCHEMA_JSON = {}
schema_json_path = os.path.join(p.parent, "isamples_simple_schema.json")
with open(schema_json_path) as schema_json_file:
    SCHEMA_JSON = json.load(schema_json_file)


def create_isamples_package(file_path: str) -> Package:
    """
    Opens the specified file and return it as a list of iSamples Core record dictionaries
    Args:
        file_path: The path to the file to open

    Returns: A list of dictionaries containing the records
    """
    data_resource = Resource(source=file_path, format="csv", mediatype="text/csv", schema=SCHEMA_JSON, trusted=True)
    package = Package(resources=[data_resource], name="isamples", title="isamples", id="isamples", trusted=True)
    return package


def things_from_isamples_package(session: Session, package: Package, max_entries: int) -> list[Thing]:
    first_resource: Resource = package.resources[0]
    num_things = 0
    url =  f"file://{first_resource.path}"
    authority_id = isb_web.config.Settings().authority_id
    things = []
    for row in first_resource:
        if 0 < max_entries <= num_things:
            break
        thing = Thing()
        thing.id = row["id"]
        thing.resolved_status = 200
        thing.tcreated = datetime.datetime.now()
        thing.resolved_url = url
        thing.authority_id = authority_id
        save_or_update_thing(session, thing)
        things.append(thing)
    return things