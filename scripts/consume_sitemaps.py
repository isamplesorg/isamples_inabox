import concurrent.futures
import urllib.parse

import click
import requests

import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging

from isb_lib.sitemaps.sitemap_fetcher import SitemapIndexFetcher, SitemapFileFetcher, ThingFetcher
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO

CONCURRENT_DOWNLOADS = 50


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
    rsession = requests.session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=CONCURRENT_DOWNLOADS, pool_maxsize=CONCURRENT_DOWNLOADS)
    rsession.mount('http://', adapter)
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
    sitemap_fetcher = SitemapIndexFetcher(url, authority, last_updated_date, rsession)
    sitemap_fetcher.fetch_index_file()
    futures = []
    more_work = len(sitemap_fetcher.urls_to_fetch) > 0
    sitemap_index_iterator = sitemap_fetcher.url_iterator()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_DOWNLOADS) as executor:
        while more_work:
            while len(futures) < CONCURRENT_DOWNLOADS:
                try:
                    url = next(sitemap_index_iterator)
                    fetcher = SitemapFileFetcher(url, authority, last_updated_date, rsession)
                    future = executor.submit(fetcher.fetch_sitemap_file)
                    futures.append(future)
                except StopIteration:
                    logging.info("Got all of the sitemap files")
                    break

            for fut in concurrent.futures.as_completed(futures):
                sitemap_file_fetcher = fut.result()
                futures.remove(fut)
                thing_futures = []
                sitemap_file_iterator = sitemap_file_fetcher.url_iterator()
                with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_DOWNLOADS) as thing_executor:
                    try:
                        thing_url = next(sitemap_file_iterator)
                        # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
                        # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
                        # We need to change full to true to get all the metadata, as well as the original format
                        parsed_url = urllib.parse.urlparse(thing_url)
                        parsed_url = parsed_url._replace(query="full=true&format=original")
                        thing_fetcher = ThingFetcher(parsed_url.geturl(), rsession)
                        thing_future = thing_executor.submit(thing_fetcher.fetch_thing)
                        thing_futures.append(thing_future)

                        for thing_fut in concurrent.futures.as_completed(thing_futures):
                            thing_fetcher = thing_fut.result()
                            thing_futures.remove(thing_fut)
                    except StopIteration:
                        logging.info(f"Finished all the requests for f{sitemap_file_fetcher._url}")


if __name__ == "__main__":
    main()
