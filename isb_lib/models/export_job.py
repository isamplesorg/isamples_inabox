from datetime import datetime
from typing import Optional

import sqlalchemy
from sqlmodel import SQLModel, Field


class ExportJob(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key",
        ),
    )
    tcreated: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the job was created.",
        index=False
    )
    tstarted: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the job was started.",
        index=True
    )
    tcompleted: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the job was completed.",
        index=True
    )
    creator_id: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The orcid of the person that created the job.",
        index=True
    )
    uuid: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Unique identifier for this export job.",
        unique=True,
        index=True
    )
    file_path: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The path to the exported file.",
        index=False
    )
