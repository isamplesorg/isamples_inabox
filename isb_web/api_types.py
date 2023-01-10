from typing import Optional

from pydantic import BaseModel

from isb_web.isb_enums import ISBAuthority, ISBReturnField

"""
Module for various API parameter and return types used throughout iSamples API
"""


class ThingsSitemapParams(BaseModel):
    identifiers: list[str]


class ReliqueryResponse(BaseModel):
    timestamp: str
    url: str
    query: str
    count: int
    return_count: int
    identifiers: list[str] = []
    description: Optional[str]


class ReliqueryParams(BaseModel):
    query: Optional[str]
    description: Optional[str]


class MintDataciteIdentifierParams(BaseModel):
    datacite_metadata: dict


class MintDraftIdentifierParams(MintDataciteIdentifierParams):
    num_drafts: int


class AddNamespaceParams(BaseModel):
    shoulder: str
    orcid_ids: list[str]


class ManageOrcidForNamespaceParams(BaseModel):
    shoulder: str
    is_remove: bool = False
    orcid_id: str


class MintNoidyIdentifierParams(BaseModel):
    shoulder: str
    num_identifiers: int
    return_filename: Optional[str] = None


class DebugTransformParams(BaseModel):
    input_record: dict
    authority: ISBAuthority
    return_field: ISBReturnField
