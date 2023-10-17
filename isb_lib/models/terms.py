"""
Store vocabulary terms and keywords.


"""

import typing

import sqlalchemy
import sqlalchemy.dialects
import sqlmodel


class Term(sqlmodel.SQLModel, table=True):
    """
    Represents the collection of vocabulary terms and keywords used by
    all entities. Terms support simple broader / narrower relationship
    (ala SKOS). Arbitrary additional properties may be associated with
    each term through the properties dictionary.
    """

    uri: str = sqlmodel.Field(
        primary_key = True,
        description="Term URI. Required. Unique across all terms."
    )
    scheme: str = sqlmodel.Field(
        description="Scheme that this term appears in.",
        index=True
    )
    name: str = sqlmodel.Field(
        description="Term value, SKOS:prefLabel",
        index=True
    )
    broader: typing.Optional[list[str]] = sqlmodel.Field(
        sa_column=sqlalchemy.Column(
            #TODO: use JSONB when we are in postgres
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="URIs of terms that are immediately broader than this term",
        ),
    )
    properties: typing.Optional[dict] = sqlmodel.Field(
        sa_column=sqlalchemy.Column(
            #TODO: use JSONB when we are in postgres
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Properties of this term."
        )
    )
