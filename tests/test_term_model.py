import pytest
import sqlmodel
import sqlmodel.pool

import isb_lib.models.repository
import isb_lib.models.terms


@pytest.fixture(name="repo")
def repo_fixture():
    engine = sqlmodel.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=sqlmodel.pool.StaticPool,
        echo=True,
    )
    sqlmodel.SQLModel.metadata.create_all(engine)
    with sqlmodel.Session(engine) as session:
        repo = isb_lib.models.repository.Repository(session)
        yield repo


def test_add_terms(repo: isb_lib.models.repository.Repository):
    term = isb_lib.models.terms.Term(
        uri="https://example.net/term/term1",
        scheme="https://example.net/term/",
        name="term1",
        properties={"foo": "baz"},
    )
    uri = repo.add_term(term)
    assert uri == term.uri
    t = repo.get_term(uri)
    assert t.uri == term.uri
    assert t.scheme == term.scheme
    assert t.name == term.name


def test_terms(repo: isb_lib.models.repository.Repository):
    scheme = "eg/s"
    terms = [
        isb_lib.models.terms.Term(uri=f"{scheme}/a", scheme=scheme, name="a"),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/b",
            scheme=scheme,
            name="b",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/c",
            scheme=scheme,
            name="c",
            broader=[
                "eg/s/b",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/bb",
            scheme=scheme,
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
    ]
    for t in terms:
        repo.add_term(t)
    repo.add_term(
        isb_lib.models.terms.Term(
            uri=f"ex/bb",
            scheme="ex",
            name="bb",
            broader=[
                "eg/s/a",
            ],
        )
    )
    res = [t for t in repo.terms(scheme)]
    assert len(res) == len(terms)
    for t in res:
        found = next((r for r in terms if r.uri == t.uri), None)
        assert found is not None


def test_schemes(repo: isb_lib.models.repository.Repository):
    scheme = "eg/s"
    for t in (
        isb_lib.models.terms.Term(uri=f"{scheme}/a", scheme=scheme, name="a"),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/b",
            scheme=scheme,
            name="b",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/c",
            scheme=scheme,
            name="c",
            broader=[
                "eg/s/b",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/bb",
            scheme=scheme,
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"ex/bb",
            scheme="ex",
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
    ):
        repo.add_term(t)
    schemes = [
        scheme,
        "ex",
    ]
    res = [t for t in repo.term_schemes()]
    assert len(res) == len(schemes)
    for s in schemes:
        found = next((r for r in res if r[0] == s), None)
        assert found is not None


def test_broader(repo: isb_lib.models.repository.Repository):
    """
           eg/s/a
         /   |    \
    ex/bb  eg/s/b  eg/s/bb
            |
          eg/s/c
    """
    scheme = "eg/s"
    for t in (
        isb_lib.models.terms.Term(uri=f"{scheme}/a", scheme=scheme, name="a"),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/b",
            scheme=scheme,
            name="b",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/c",
            scheme=scheme,
            name="c",
            broader=[
                "eg/s/b",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/bb",
            scheme=scheme,
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"ex/bb",
            scheme="ex",
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
    ):
        repo.add_term(t)
    expected = ["eg/s/c", "eg/s/b", "eg/s/a"]
    for t in repo.broader_terms("eg/s/c"):
        assert t[0] in expected


def test_narrower(repo: isb_lib.models.repository.Repository):
    """
           eg/s/a
         /   |    \
    ex/bb  eg/s/b  eg/s/bb
            |
          eg/s/c
    """
    scheme = "eg/s"
    for t in (
        isb_lib.models.terms.Term(uri=f"{scheme}/a", scheme=scheme, name="a"),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/b",
            scheme=scheme,
            name="b",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/c",
            scheme=scheme,
            name="c",
            broader=[
                "eg/s/b",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"{scheme}/bb",
            scheme=scheme,
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
        isb_lib.models.terms.Term(
            uri=f"ex/bb",
            scheme="ex",
            name="bb",
            broader=[
                "eg/s/a",
            ],
        ),
    ):
        repo.add_term(t)
    expected = ["eg/s/b", "eg/s/c", "eg/s/bb", "ex/bb"]
    result = [t[0] for t in repo.narrower_terms("eg/s/a")]
    for t in result:
        assert t in expected
    assert len(expected) == len(result)
