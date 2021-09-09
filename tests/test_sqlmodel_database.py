import datetime

import pytest
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import get_thing_with_id, read_things, last_time_thing_created


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def test_get_thing_with_id_no_things(session: Session):
    should_be_none = get_thing_with_id(session, "12345")
    assert should_be_none is None


def test_get_thing_with_id_thing(session: Session):
    id = "123456"
    new_thing = Thing(id=id, authority_id="test", resolved_url="http://foo.bar", resolved_status=200, resolved_content = { "foo": "bar" })
    session.add(new_thing)
    session.commit()
    shouldnt_be_none = get_thing_with_id(session, id)
    assert shouldnt_be_none is not None
    assert shouldnt_be_none.primary_key is not None


def test_read_things_no_things(session: Session):
    count, pages, data = read_things(session, 0, 0)
    assert 0 == count
    assert 0 == pages
    assert len(data) == 0


def test_read_things_with_things(session: Session):
    new_thing = Thing(id="123456", authority_id="test", resolved_url="http://foo.bar", resolved_status=200, resolved_content = { "foo": "bar" })
    session.add(new_thing)
    session.commit()
    count, pages, data = read_things(session, 0, 0)
    assert 1 == count
    assert 0 == pages
    assert len(data) > 0


def test_last_time_thing_created(session: Session):
    test_authority = "test"
    created = last_time_thing_created(session, test_authority)
    assert created is None
    new_thing = Thing(id="123456", authority_id=test_authority, resolved_url="http://foo.bar", resolved_status=200,
                      resolved_content={"foo": "bar"}, tcreated=datetime.datetime.now())
    session.add(new_thing)
    session.commit()
    new_created = last_time_thing_created(session, test_authority)
    assert new_created is not None
