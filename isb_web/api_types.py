from typing import Optional

from pydantic import BaseModel


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
    # The following fields are only present if a query is re-executed:
    previous_timestamp: Optional[str]
    previous_url: Optional[str]
    previous_count: Optional[int]


class ReliqueryParams(BaseModel):
    query: Optional[str]
    description: Optional[str]
    # Useful to re-run a previously run reliquery
    previous_response: Optional[ReliqueryResponse]
