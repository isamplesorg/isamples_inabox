import datetime
import lxml.etree
import requests
import dateparser
import typing


class SitemapFetcher:
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

    def fetch_index_file(self):
        res = self._session.get(self._url)
        xmlp = lxml.etree.XMLParser(
            recover=True,
            remove_comments=True,
            resolve_entities=False,
        )
        root = lxml.etree.fromstring(res.content, parser=xmlp)
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
