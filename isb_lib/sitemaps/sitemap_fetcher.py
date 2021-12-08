from abc import ABC
import datetime
import lxml.etree
import requests
import dateparser
import typing
import logging
from isb_lib.models.thing import Thing


class ThingFetcher:
    def __init__(self, url: str, session: requests.sessions = requests.session()):
        self._url = url
        self._session = session
        self.thing = None

    def fetch_thing(self):
        try:
            response = self._session.get(self._url)
            json_dict = response.json()
            thing = Thing()
            thing.take_values_from_json_dict(json_dict)
            self.thing = thing
        except Exception as e:
            logging.warning(e)
            return None


class SitemapFetcher(ABC):
    def __init__(
        self,
        url: str,
        authority: str,
        last_modified: typing.Optional[datetime.datetime],
        session: requests.sessions = requests.session(),
    ):
        self._url = url
        self._authority = authority
        self._last_modified = last_modified
        self._session = session
        self.urls_to_fetch = []

    def _fetch_file(self):
        res = self._session.get(self._url)
        root = lxml.etree.fromstring(res.content)
        sitemap_list = root.getchildren()
        """These sitemap children look like this:
              <sitemap>
                <loc>http://mars.cyverse.org/sitemaps/sitemap-5.xml</loc>
                <lastmod>2006-08-10T12:00:00Z</lastmod>
              </sitemap>    
              or this:
                <urlset>
                  <url>
                    <loc>thing/ark:/28722/k2bg30w29?full=false&amp;format=core</loc>
                    <lastmod>2021-07-02T22:49:54Z</lastmod>
                  </url>
                </urlset>
            Either way, we can parse them the same way              
        """
        for sitemap_child in sitemap_list:
            loc = (
                sitemap_child.iterchildren(
                    "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                )
                .__next__()
                .text
            )
            lastmod = (
                sitemap_child.iterchildren(
                    "{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod"
                )
                .__next__()
                .text
            )
            lastmod_date = dateparser.parse(lastmod)
            if (
                self._last_modified is None
                or lastmod_date.timestamp() >= self._last_modified.timestamp()
            ):
                self.urls_to_fetch.append(loc)


class SitemapFileFetcher(SitemapFetcher):
    def fetch_sitemap_file(self):
        self._fetch_file()

    def fetch_child_files(self) -> typing.List[ThingFetcher]:
        thing_fetchers = []
        for url in self.urls_to_fetch:
            thing_fetcher = ThingFetcher(
                self.prepare_thing_file_url(url), self._session
            )
            thing_fetcher.fetch_thing()
            thing_fetchers.append(thing_fetcher)
        return thing_fetchers

    def prepare_thing_file_url(self, file_url: str) -> str:
        """Mainly used as a placeholder for overriding in unit testing"""
        return file_url


class SitemapIndexFetcher(SitemapFetcher):
    def fetch_index_file(self):
        xmlp = lxml.etree.XMLParser(
            recover=True,
            remove_comments=True,
            resolve_entities=False,
        )
        lxml.etree.set_default_parser(xmlp)
        self._fetch_file()

    def fetch_child_files(self) -> typing.List[SitemapFileFetcher]:
        file_fetchers = []
        for url in self.urls_to_fetch:
            child_file_fetcher = self.sitemap_file_fetcher(url)
            child_file_fetcher.fetch_sitemap_file()
            file_fetchers.append(child_file_fetcher)
        return file_fetchers

    def prepare_sitemap_file_url(self, file_url: str) -> str:
        """Mainly used as a placeholder for overriding in unit testing"""
        return file_url

    def sitemap_file_fetcher(self, url: str) -> SitemapFileFetcher:
        return SitemapFileFetcher(
            self.prepare_sitemap_file_url(url),
            self._authority,
            self._last_modified,
            self._session,
        )
