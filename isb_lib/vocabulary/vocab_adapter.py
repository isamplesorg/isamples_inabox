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
import logging
from term_store import TermRepository
from term_store.db import Term

VOCAB_CACHE: dict = {}


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


def uijson_vocabulary_dict(top_level_uri: str, repository: TermRepository) -> dict:
    cached_result = VOCAB_CACHE.get(top_level_uri)
    if cached_result is not None:
        return cached_result
    root_term = repository.read(top_level_uri)
    if root_term is None:
        logging.warning(f"Expected to find root term with uri {top_level_uri}, found None instead.")
        return {}
    else:
        full_dict = _read_descendants(root_term, repository)
        VOCAB_CACHE[top_level_uri] = full_dict
        return full_dict
