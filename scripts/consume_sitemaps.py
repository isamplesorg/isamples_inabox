import datetime
import click
import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import lxml.etree
import requests
import dateparser
import logging


class SitemapFetcher:
    def __init__(self, url: str, authority: str, last_modified: datetime.datetime):
        self._url = url
        self._authority = authority
        self._last_modified = last_modified
        self.urls_to_fetch = []

    def fetch_index_file(self):
        res = requests.session().get(self._url)
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
            loc = sitemap_child.iterchildren("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").__next__().text
            lastmod = sitemap_child.iterchildren("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod").__next__().text
            lastmod_date = dateparser.parse(lastmod)
            if self._last_modified is None or lastmod_date >= self._last_modified:
                self.urls_to_fetch.append(loc)


@click.command()
@click.pass_context
@click.option(
    "-u",
    "--url",
    type=str,
    default=None,
    help="The URL to the sitemap index file to consume",
)
@click.option(
    "-a",
    "--authority",
    type=str,
    default=None,
    help="The authority used for storing the retrieved records",
)
@click.option(
    "-i",
    "--ignore_last_modified",
    is_flag=True,
    help="Whether to ignore the last modified date and do a full rebuild"
)
def main(ctx, url: str, authority: str, ignore_last_modified: bool):
    solr_url = isb_web.config.Settings().solr_url
    authority = authority.upper()
    isb_lib.core.things_main(
        ctx, None, solr_url, "INFO", False
    )
    if ignore_last_modified:
        max_solr_updated_date = None
    else:
        max_solr_updated_date = isb_lib.core.solr_max_source_updated_time(
            url=solr_url,
            authority_id=authority.upper(),
        )
    sitemap_fetcher = SitemapFetcher(url, authority, max_solr_updated_date)
    sitemap_fetcher.fetch_index_file()
    logging.info(f"Going to fetch {len(sitemap_fetcher.urls_to_fetch)} sitemap URLs")


if __name__ == "__main__":
    main()