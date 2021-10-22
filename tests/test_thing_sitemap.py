import pytest
from sqlmodel import Session, SQLModel, create_engine, select
from sqlmodel.pool import StaticPool

from isb_lib.sitemaps.thing_sitemap import SitemapIndexIterator
from test_utils import _add_some_things


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
    num_things = 10
    _add_some_things(session, num_things, "test_authority")
    iterator = SitemapIndexIterator(session, None, 1)
    for urlset_iterator in iterator:
        entries_for_urlset = []
        for urlset_entry in urlset_iterator:
            entries_for_urlset.append(urlset_entry)
        assert 1 == urlset_iterator.num_urls
    # chunked to 1 per file, so we should have a total of 10 files
    assert iterator.num_url_sets == 10