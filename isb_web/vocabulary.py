import logging
from typing import Optional

import term_store
from fastapi import APIRouter, Depends
from sqlmodel import Session

from isb_lib.vocabulary import vocab_adapter
from isb_web.sqlmodel_database import SQLModelDAO

router = APIRouter(prefix="/vocabulary")
dao: Optional[SQLModelDAO] = None
logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("metrics")


def get_session():
    with dao.get_session() as session:
        yield session


@router.get("/material_sample_type")
def material_sample_type(session: Session  = Depends(get_session)) -> dict:
    repository = term_store.get_repository(session)
    return vocab_adapter.uijson_vocabulary_dict("https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen", repository)

@router.get("/material_type")
def material_type(session: Session  = Depends(get_session)) -> dict:
    repository = term_store.get_repository(session)
    return vocab_adapter.uijson_vocabulary_dict("https://w3id.org/isample/vocabulary/material/1.0/material", repository)

@router.get("/sampled_feature_type")
def sampled_feature_type(session: Session  = Depends(get_session)) -> dict:
    repository = term_store.get_repository(session)
    return vocab_adapter.uijson_vocabulary_dict("https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature", repository)