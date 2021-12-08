import urllib.parse

import click
import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging

from isb_lib.sitemaps.sitemap_fetcher import SitemapIndexFetcher
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO


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
    db_session = SQLModelDAO(isb_web.config.Settings().database_url).get_session()
    authority = authority.upper()
    isb_lib.core.things_main(
        ctx, None, solr_url, "INFO", False
    )
    if ignore_last_modified:
        last_updated_date = None
    else:
        last_updated_date = sqlmodel_database.last_time_thing_created(
            db_session, authority
        )
    sitemap_fetcher = SitemapIndexFetcher(url, authority, last_updated_date)
    sitemap_fetcher.fetch_index_file()
    for child_file in sitemap_fetcher.fetch_child_files():
        # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
        # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
        # We need to change full to true to get all the metadata, as well as the original format
        rewritten_urls = []
        for url in child_file.urls_to_fetch:
            parsed_url = urllib.parse.urlparse(url)
            parsed_url.query = "full=true&format=original"
            rewritten_urls.append(parsed_url.geturl())
        child_file.urls_to_fetch = rewritten_urls
    logging.info(f"Going to fetch {len(sitemap_fetcher.urls_to_fetch)} sitemap URLs")


if __name__ == "__main__":
    main()