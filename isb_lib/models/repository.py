"""
Implements repository pattern for get/set database content.
"""
import typing
import sqlmodel
import sqlalchemy.orm
from isb_lib.models.terms import Term


class Repository:
    """
    Implements a repository pattern for interacting with the things database.

    Currently supports only Term instances.

    General pattern of use is something like:
        engine = create_engine(db_connection_string)
        session = sqlalchemy.orm.sessionmaker(bind=engine)()
        try:
            repo = Repository(session)
            #do stuff with repo
        finally:
            session.close()

    In a web application, sessions are usually per request, engine is lifespan of app. e.g.

    _engine = None

    def get_engine():
        global _engine
        if _engine is None:
            _engine = sqlalchemy.create_engine(cn_str)
        return _engine

    def repo_lifespan(application:FastAPI):
        engine = get_engine()
        yield
        engine.dispose()

    def get_repo():
        session = sqlalchemy.orm.sessionmaker(_engine)()
        repo = Repository(session)
        try:
            yield repo
        finally:
            session.close()

    app = FastAPI(lifespan=repo_lifespan)

    @app.get("/term/{uri:path}")
    def web_get_term(
        request:fastapi.Request,
        uri:str,
        repo: Repository = fastapi.Depends(get_repo)
    )
        return repo.get_term(uri)
    """

    def __init__(self, session: sqlmodel.Session):
        self.session = session

    def add_term(self, term: Term) -> str:
        self.session.add(term)
        self.session.commit()
        return term.uri

    def get_term(self, term_uri: str) -> Term:
        term = self.session.get(Term, term_uri)
        return term

    def terms(self, scheme: str):
        q = sqlmodel.select(Term).where(Term.scheme == scheme)
        for t in self.session.exec(q):
            yield t

    def broader_terms(self, start_uri: str) -> typing.Generator[Term, None, None]:
        """Yield terms that are broader than the provided URI

        The SQL is a recursive common table expression that traverses the term hierarchy.
        Equivalent plain SQL (note that for postgres the LIKE clause should ideally be
        replaced with array search):

        WITH RECURSIVE cte(uri, broader) AS (
            SELECT
                term.uri AS uri,
                term.broader AS broader,
            FROM term WHERE term.uri = :start_uri
            UNION SELECT
                term.uri AS term_uri,
                term.broader AS term_broader,
            FROM term
            JOIN cte ON (
                cte.broader LIKE '%' || term.uri || '%'
            )
        ) SELECT cte.uri AS cte_uri,
                 cte.broader AS cte_broader,
        FROM cte

        #qq = self._session.execute(sql, {"start_uri":start_uri})
        """
        # sqlmodel does not seem to support recursive CTE, so fall back to sqlalchemy
        q = sqlalchemy.select(Term.uri, Term.broader)
        q = q.filter(Term.uri == start_uri)
        q = q.cte("cte", recursive=True)
        q2 = sqlalchemy.select(Term.uri, Term.broader)
        q2 = q2.join(q, q.c.broader.contains(Term.uri))
        rq = q.union(q2)
        for record in self.session.query(rq):
            yield record

    def narrower_terms(self, start_uri) -> typing.Generator[Term, None, None]:
        """Yield terms that are narrower than start_uri."""
        q = sqlalchemy.select(Term.uri, Term.broader)
        q = q.filter(Term.broader.contains(start_uri))
        q = q.cte("cte", recursive=True)
        q2 = self.session.query(Term.uri, Term.broader)
        q2 = q2.join(q, Term.broader.contains(q.c.uri))
        rq = q.union(q2)
        for record in self.session.query(rq):
            yield record

    def term_schemes(self) -> typing.Generator[typing.Tuple[str, int], None, None]:
        """Yield tuples of (scheme, term count per scheme) in the repository."""
        sql = sqlalchemy.text(
            "SELECT scheme, count(*) AS cnt FROM term GROUP BY scheme;"
        )
        # q = self._session.query(sqlalchemy.distinct(Term.scheme), sqlalchemy.func.count(Term.scheme))
        q = self.session.execute(sql)
        for record in q:
            yield (record)
