from abc import ABC
import datetime
import lxml.etree
import requests
import dateparser
import typing


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
            child_file_fetcher = SitemapFileFetcher(self.prepare_sitemap_file_url(url), self._authority, self._last_modified, self._session)
            child_file_fetcher.fetch_sitemap_file()
            file_fetchers.append(child_file_fetcher)
        return file_fetchers

    def prepare_sitemap_file_url(self, file_url: str) -> str:
        """Mainly used as a placeholder for overriding in unit testing"""
        return file_url
