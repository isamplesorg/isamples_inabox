import datetime
import typing

from sqlmodel import Session

from isb_lib.models.thing import Thing
from isb_web import sqlmodel_database

MAX_URLS_IN_SITEMAP = 50000


class SitemapIndexEntry:
    """Individual sitemap file entry in the sitemap index"""

    def __init__(self, sitemap_filename: str, last_mod: datetime.datetime):
        self.sitemap_filename = sitemap_filename
        self.last_mod_str = last_mod.isoformat()


class UrlSetEntry:
    """Individual url entry in an urlset"""

    def __init__(self, identifier: str, last_mod: datetime.datetime):
        self.identifier = identifier
        self.last_mod_str = last_mod.isoformat()


class UrlSetIterator:
    """Iterator class responsible for listing individual urls in an urlset"""

    def __init__(self, sitemap_index: int, max_length: int, things: typing.List[Thing]):
        self._things = things
        self._thing_index = 0
        self._max_length = max_length
        self.num_urls = 0
        self.sitemap_index = sitemap_index
        self.last_tstamp = None
        self.last_primary_key = None

        self.entries = []

    def __iter__(self):
        return self

    @staticmethod
    def _entries_for_thing(thing: Thing) -> typing.List[UrlSetEntry]:
        # TODO: this needs to account for child records
        return [UrlSetEntry(thing.id, thing.tstamp)]

    def _has_next(self, thing_entries: typing.List[UrlSetEntry]) -> bool:
        return self.num_urls + len(thing_entries) <= self._max_length

    def __next__(self) -> UrlSetEntry:
        # Dont read past the bounds
        if self._thing_index == len(self._things) or self.num_urls > self._max_length:
            raise StopIteration
        # Safe to check the length by creating the urlset entries to see if we're past the end
        next_thing = self._things[self._thing_index]
        thing_entries = self._entries_for_thing(next_thing)
        if not self._has_next(thing_entries):
            raise StopIteration
        else:
            # Update the necessary state
            self.num_urls += 1
            self._thing_index += 1
            self.last_tstamp = next_thing.tstamp
            self.last_primary_key = next_thing.primary_key
            # TODO: handle the case where we have multiple Things
            return thing_entries[0]

    def sitemap_index_entry(self) -> SitemapIndexEntry:
        return SitemapIndexEntry(f"sitemap-{self.sitemap_index}.xml", self.last_tstamp)


class SitemapIndexIterator:
    """Iterator class responsible for listing the individual sitemap files in a sitemap index"""

    def __init__(
        self,
        session: Session,
        authority: str = None,
        num_things_per_file: int = MAX_URLS_IN_SITEMAP,
        status: int = 200,
        offset: int = 0,
    ):
        self._last_timestamp = None
        self._last_primary_key = 0
        self._session = session
        self._authority = authority
        self._num_things_per_file = num_things_per_file
        self._status = status
        self._offset = offset
        self._last_url_set_iterator = None
        self.num_url_sets = 0

    def __iter__(self):
        return self

    def __next__(self) -> UrlSetIterator:
        if self._last_url_set_iterator is not None:
            # Update our last values with the last ones from the previous iterator
            self._last_timestamp = self._last_url_set_iterator.last_tstamp
            self._last_primary_key = self._last_url_set_iterator.last_primary_key
        things = sqlmodel_database.things_for_sitemap(
            self._session,
            self._authority,
            self._status,
            self._num_things_per_file,
            self._offset,
            self._last_timestamp,
            self._last_primary_key,
        )
        # TODO: this needs to handle edge cases
        if len(things) == 0:
            raise StopIteration
        next_url_set_iterator = UrlSetIterator(self.num_url_sets, self._num_things_per_file, things)
        self._last_url_set_iterator = next_url_set_iterator
        self.num_url_sets = self.num_url_sets + 1
        return next_url_set_iterator

