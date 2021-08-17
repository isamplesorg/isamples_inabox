import click
import click_config_file
import isb_lib.core
from sqlalchemy import select
from sqlalchemy import update
import igsn_lib.models
import igsn_lib.models.thing
import dateparser


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
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="opencontext.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate):
    isb_lib.core.things_main(ctx, db_url, verbosity, heart_rate)
    session = isb_lib.core.get_db_session(db_url)
    index = 0
    page_size = 10000
    max_index = 850000
    count = 0
    while index < max_index:
        iterator = session.execute(
            select(
                igsn_lib.models.thing.Thing._id,
                igsn_lib.models.thing.Thing.resolved_content,
            ).where(igsn_lib.models.thing.Thing.authority_id == "OPENCONTEXT")
            .slice(index, index + page_size)
        )
        for row in iterator:
            dict = row._asdict()
            id = dict["_id"]
            resolved_content = dict["resolved_content"]
            updated = dateparser.parse(resolved_content["updated"])
            print(f"row is {dict}")
            count += 1
            session.execute(
                update(igsn_lib.models.thing.Thing)
                .where(igsn_lib.models.thing.Thing._id == id)
                .values(tcreated=updated)
            )
        session.commit()
        index += page_size
    print(f"num records is {count}")

"""
Updates existing OpenContext records in a Things db to have their tcreated column based on the OpenContext "updated"
field in the JSON as opposed to the previous implementation, which used the OpenContext "published" field.
"""
if __name__ == "__main__":
    main()
