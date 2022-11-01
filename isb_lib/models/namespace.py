from datetime import datetime
from typing import Optional

import igsn_lib
import sqlalchemy
from sqlmodel import SQLModel, Field

from isb_lib.models.string_list_type import StringListType


class Namespace(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key, good for paging",
        ),
    )
    shoulder: Optional[str] = Field(
        # The shoulder for the namespace, e.g. "1234/fk44" in "ark:1234/fk44w2w"
        sa_column=sqlalchemy.Column(
            "shoulder", sqlalchemy.String, unique=True, doc="The string value for the namespace's shoulder"
        )
    )
    allowed_people: Optional[list[str]] = Field(
        sa_column=sqlalchemy.Column(
            StringListType,
            nullable=True,
            default=None,
            doc="A list of orcid ids that have permission to mint identifiers in this namespace"
        )
    )
    tcreated: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the namespace was created.",
    )
    tstamp: datetime = Field(
        default=igsn_lib.time.dtnow(),
        description="The last time anything was modified in this namespace",
    )
    minter_content: Optional[dict] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Internal state of the minter associated with this namespace",
        ),
    )
