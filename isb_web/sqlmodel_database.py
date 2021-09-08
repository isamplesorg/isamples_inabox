from typing import Optional, List
from sqlmodel import SQLModel, create_engine, Session, select
from isb_lib.models.thing import Thing
from isb_web.schemas import ThingPage
import isb_web.config


class SQLModelDAO:
    def __init__(self, db_url: str):
        # This is a strange initializer, but FastAPI wants us to construct the object before we know we
        # want to use it.  So, separate out the object construction from the database connection.
        # In unit tests, this ends up getting swapped out and unused, which is the source of the confusion.
        if db_url is not None:
            self.connect_sqlmodel(db_url)

    def connect_sqlmodel(self, db_url: str):
        self.engine = create_engine(db_url, echo=True)
        SQLModel.metadata.create_all(self.engine)

    def get_session(self) -> Session:
        return Session(self.engine)


def read_things(
    session: Session,
    offset: int,
    limit: int,
    status: int = 200,
    authority: Optional[str] = None,
) -> tuple[int, int, List[ThingPage]]:
    count_statement = session.query(Thing)
    if authority is not None:
        count_statement = count_statement.filter(Thing.authority_id == authority)
    count_statement = count_statement.filter(Thing.resolved_status == status)
    overall_count = count_statement.count()
    overall_pages = overall_count / limit
    things_statement = select(
        Thing.primary_key,
        Thing.id,
        Thing.authority_id,
        Thing.tcreated,
        Thing.resolved_status,
        Thing.resolved_url,
        Thing.resolve_elapsed,
    )
    if authority is not None:
        things_statement = things_statement.filter(Thing.authority_id == authority)
    things_statement = things_statement.filter(Thing.resolved_status == status)
    things_statement = things_statement.offset(offset)
    things_statement = things_statement.limit(limit)
    things_results = session.exec(things_statement)
    return overall_count, overall_pages, things_results.all()


def get_thing(session: Session, identifier: str) -> Optional[Thing]:
    statement = select(Thing).filter(Thing.id == identifier)
    return session.exec(statement).first()
