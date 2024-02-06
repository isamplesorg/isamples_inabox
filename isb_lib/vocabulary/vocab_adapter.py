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
import json
import term_store
from term_store import TermRepository
from term_store.db import Term

from isb_web.sqlmodel_database import SQLModelDAO


def _read_descendants(term: Term, repository: TermRepository) -> dict:
    term_dict = {}
    label = term.properties.get("labels")
    if label is not None:
        label = label[0]
    else:
        label = term.name
    children: list[dict] = []
    term_dict[term.uri] = {
        "label": {
            "en": label
        },
        "children": children
    }
    descendants = repository.narrower(term.uri)
    for descendant in descendants:
        child_dict = _read_descendants(descendant, repository)
        children.append(child_dict)
    return term_dict


@functools.lru_cache(maxsize=10)
def uijson_vocabulary_dict(top_level_uri: str, repository: TermRepository) -> dict:
    root_term = repository.read(top_level_uri)
    if root_term is None:
        logging.warning(f"Expected to find root term with uri {top_level_uri}, found None instead.")
        return {}
    else:
        return _read_descendants(root_term, repository)


if __name__ == "__main__":
    dao = SQLModelDAO("postgresql+psycopg2://isb_writer:isamplesinabox@localhost/isb_2")
    session = term_store.get_session(dao.engine) # type: ignore
    repository = term_store.get_repository(session)
    vocab_dict = uijson_vocabulary_dict("https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen", repository)
    print(json.dumps(vocab_dict, indent=2))
