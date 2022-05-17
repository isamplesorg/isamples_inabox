import datetime
from typing import Any, Optional


class Identifier:
    def __init__(
        self,
        identifier: str,
        creators: [str],
        titles: [str],
        publisher: str,
        publication_year: int = datetime.datetime.now().year,
        resource_type: str = "PhysicalObject",
    ):
        self._identifier = identifier
        self._creators = creators
        self._titles = titles
        self._publisher = publisher
        self._publication_year = publication_year
        self._resource_type = resource_type

    def metadata_dict(self) -> dict[str: Any]:
        pass


class DataciteIdentifier(Identifier):
    def __init__(
        self,
        doi: Optional[str],
        prefix: Optional[str],
        creators: [str],
        titles: [str],
        publisher: str,
        resource_type: str = "PhysicalObject",
    ):
        if doi is None and prefix is None:
            raise ValueError("One of doi or prefix must be specified.")
        if len(creators) == 0:
            raise ValueError("One or more creators must be specified.")
        if len(titles) == 0:
            raise ValueError("One or more titles must be specified.")

        if doi is not None:
            super().__init__(doi, creators, titles, publisher, resource_type)
            self._is_doi = True
        else:
            super().__init__(prefix, creators, titles, publisher, resource_type)
            self._is_doi = False

    def metadata_dict(self) -> dict[str: Any]:
        metadata_dict = {}
        if self._is_doi:
            metadata_dict["doi"] = self._identifier
        else:
            metadata_dict["prefix"] = self._identifier
        metadata_dict["types"] = {"resourceTypeGeneral": self._resource_type}
        metadata_dict["creators"] = self._creators
        titles = []
        for title in self._titles:
            titles.append({"title": title})
        metadata_dict["titles"] = titles
        metadata_dict["publisher"] = self._publisher
        metadata_dict["publicationYear"] = self._publication_year
        return metadata_dict


class IGSNIdentifier(DataciteIdentifier):
    pass
