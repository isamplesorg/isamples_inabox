from typing import Optional, List
from sqlmodel import Session, select
from isb_lib.models.thing import Thing
from isb_web.schemas import ThingPage


def read_things(
    session: Session,
    offset: int,
    limit: int,
    status: int = 200,
    authority: Optional[str] = None
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
