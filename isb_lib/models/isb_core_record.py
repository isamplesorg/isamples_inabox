from typing import Optional

from sqlmodel import SQLModel, Field


class ISBCoreRecord(SQLModel, table=True):
    id: Optional[str] = Field(
        default=None,
        nullable=False,
        primary_key=True,
        description="The identifier in the original data, which should be a unique identifier (e.g. ark or igsn)"
    )
    isb_core_id: Optional[str] = Field(
        default=None,
        nullable=False,
        description="The iSamples persistent identifier, which is how the sample is uniquely identified in iSamples"
    )
    isb_updated_time: Optional[str] = Field(
        default=None,
        nullable=False,
        description="The time the record was last updated in the iSamples index",
        index=False
    )
    source_updated_time: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The time the record was last updated in the original data, if available",
        index=False
    ),
    label: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A label for the record",
        index=False
    ),
    search_text: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A collection of text suitable for use in a full-text search index, semicolon delimited",
        index=False
    ),
    sample_description: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A textual description of the sample",
        index=False
    ),
    context_categories: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The context categories from the iSamples controlled vocabulary, semicolon delimited",
        index=False
    ),
    material_categories: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The material categories from the iSamples controlled vocabulary, semicolon delimited",
        index=False
    ),
    specimen_categories: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The material categories from the iSamples controlled vocabulary, semicolon delimited",
        index=False
    ),
    keywords: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Keywords for searching, semicolon delimited",
        index=False
    ),
    informal_classification: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Informal scientific classification of the sample",
        index=False
    ),
    produced_by_isb_core_id: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The iSB Core ID of the sample that produced this sample",
        index=False
    ),
    produced_by_label: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A label for how the sample was produced",
        index=False
    ),
    produced_by_description: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A text description for how the sample was produced",
        index=False
    ),
    produced_by_feature_of_interest: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A string specifying whether the sample was produced by a feature of interest",
        index=False
    ),
    produced_by_responsibility: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The name of the people or institution responsible for producing the sample, semicolon-delimited",
        index=False
    ),
    produced_by_result_time: Optional[str] = Field(
        default=None,
        nullable=True,
        description="ISO8601 textual representation of the time the sample was collected",
        index=False
    ),
    produced_by_sampling_site_description: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A textual description of the sampling site",
        index=False
    ),
    produced_by_sampling_site_label: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A label for the sampling site",
        index=False
    ),
    produced_by_sampling_site_elevation_in_meters: Optional[float] = Field(
        default=0.0,
        nullable=True,
        description="The elevation in meters of the sampling site",
        index=False
    ),
    produced_by_sampling_site_place_name: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The place name of the sampling site",
        index=False
    ),
    registrant: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The registrant of the sample",
        index=False
    ),
    sampling_purpose: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Why the sample was collected",
        index=False
    ),
    curation_label: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A label for the sample used during curation",
        index=False
    ),
    curation_description: Optional[str] = Field(
        default=None,
        nullable=True,
        description="A textual description of how the sample was curated",
        index=False
    ),
    curation_access_constraints: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Access constraints around the sample curation",
        index=False
    ),
    curation_location: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Where the sample is curated",
        index=False
    ),
    curation_responsibility: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The institution or people responsible for curation",
        index=False
    ),
    related_resources_isb_core_id: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Identifiers for related resources, semicolon-delimited",
        index=False
    ),