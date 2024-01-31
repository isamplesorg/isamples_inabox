"""Module for formatting vocabulary terms in a format suitable for iSamples UI ingestion, e.g.

{
  "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen": {
    "label": {
      "en": "Physical specimen"
    },
    "children": [
    …
    ]
}
"""
import functools
import logging

import sqlalchemy
import term_store
from sqlalchemy import JSON
from term_store import TermRepository
from term_store.db import Term

from isb_web.sqlmodel_database import SQLModelDAO


def _read_descendants(term: Term, repository: TermRepository):
    print(f"looking at term {term.uri}")
    descendants = repository.narrower(term.uri)
    for descendant in descendants:
        _read_descendants(descendant, repository)


@functools.lru_cache(maxsize=10)
def uijson_vocabulary_dict(top_level_uri: str, repository: TermRepository) -> dict:
    root_term = repository.read(top_level_uri)
    if root_term is None:
        logging.warning(f"Expected to find root term with uri {top_level_uri}, found None instead.")
        return {}
    else:
        _read_descendants(root_term, repository)


if __name__ == "__main__":
    dao = SQLModelDAO("postgresql+psycopg2://isb_writer:isamplesinabox@localhost/isb_2")
    session = term_store.get_session(dao.engine)
    repository = term_store.get_repository(session)
    uijson_vocabulary_dict("https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen", repository)