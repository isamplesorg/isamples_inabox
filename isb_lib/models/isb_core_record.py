from typing import Optional

from sqlmodel import SQLModel, Field


class ISBCoreRecord(SQLModel, table=True):
    id: Optional[str] = Field(
        default=None,
        nullable=False,
        primary_key=True,
        description="Solr ID, which should be a unique identifier (e.g. ark or igsn)"
    )