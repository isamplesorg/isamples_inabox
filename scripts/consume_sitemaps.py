import concurrent.futures
import typing
import urllib.parse

import click
import httpx
import requests

import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging
from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
    ThingFetcher,
)
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO
from isb_lib.models.thing import Thing
from itertools import islice
import asyncio

CONCURRENT_DOWNLOADS = 100


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
    help="Whether to ignore the last modified date and do a full rebuild",
)
def main(ctx, url: str, authority: str, ignore_last_modified: bool):
    solr_url = isb_web.config.Settings().solr_url
    rsession = requests.session()
    # adapter = requests.adapters.HTTPAdapter(
    #     pool_connections=CONCURRENT_DOWNLOADS * 2, pool_maxsize=CONCURRENT_DOWNLOADS * 2
    # )
    # rsession.mount("http://", adapter)
    db_url = isb_web.config.Settings().database_url
    db_session = SQLModelDAO(db_url).get_session()
    if authority is not None:
        authority = authority.upper()
    isb_lib.core.things_main(ctx, db_url, solr_url, "INFO", False)
    if ignore_last_modified:
        last_updated_date = None
    else:
        # Smithsonian's dump has dates marked in the future.  So, Smithsonian will never update.  For the purposes
        # of iSamples Central, this is actually ok as we don't have an automated import pipeline for Smithsonian.
        # Once the Smithsonian gets an automated import time in place, we'll need to address this somehow.
        # https://github.com/isamplesorg/isamples_inabox/issues/110
        last_updated_date = sqlmodel_database.last_time_thing_created(
            db_session, authority
        )
    logging.info(f"Going to fetch records for authority {authority} with updated date > {last_updated_date}")
    fetch_sitemap_files(authority, last_updated_date, rsession, url, db_session)


def massaged_url_for_url(thing_url: str) -> str:
    # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to change full to true to get all the metadata, as well as the original format
    parsed_url = urllib.parse.urlparse(thing_url)
    parsed_url = parsed_url._replace(query="full=true&format=original")
    massaged_url = parsed_url.geturl()
    logging.info(f"Return massaged url for {massaged_url}")
    return massaged_url


async def thing(url) -> typing.Tuple[Thing, str, str]:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    json_dict = response.json()
    fetched_thing = Thing()
    fetched_thing.take_values_from_json_dict(json_dict)
    return fetched_thing, json_dict["primary_key"], url


async def get_records(urls: typing.List[str]):
    def _rmap(url):
        return thing(massaged_url_for_url(url))
    return await asyncio.gather(*map(_rmap, urls))


def fetch_sitemap_files(authority, last_updated_date, rsession, url, db_session):
    sitemap_index_fetcher = SitemapIndexFetcher(
        url, authority, last_updated_date, rsession
    )
    # fetch the index file, and iterate over the individual sitemap files serially so we preserve order
    num_things_fetched = 0
    sitemap_index_fetcher.fetch_index_file()
    for url in sitemap_index_fetcher.urls_to_fetch:
        things_for_file = 0
        sitemap_file_fetcher = SitemapFileFetcher(
            url, authority, last_updated_date, rsession
        )
        sitemap_file_fetcher.fetch_sitemap_file()
        # sitemap_file_iterator = sitemap_file_fetcher.url_iterator()
        # iterator = iter(sitemap_file_fetcher.urls_to_fetch)
        while things_for_file < len(sitemap_file_fetcher.urls_to_fetch):
            batch = sitemap_file_fetcher.urls_to_fetch[things_for_file: things_for_file + CONCURRENT_DOWNLOADS]
            for (thing, primary_key, url) in asyncio.run(get_records(batch)):
                if thing is not None:
                    logging.info(
                        f"Finished fetching thing {primary_key}"
                    )
                    num_things_fetched += 1
                    things_for_file += 1
                    if (
                            primary_key
                            not in sitemap_index_fetcher.primary_keys_fetched
                    ):
                        sqlmodel_database.save_or_update_thing(
                            db_session, thing
                        )
                        sitemap_index_fetcher.primary_keys_fetched.add(
                            primary_key
                        )
                    if num_things_fetched % 1000 == 0:
                        logging.info(f"Have fetched {num_things_fetched} things")
                else:
                    logging.error(f"Error fetching thing for {url}")
                    sqlmodel_database.mark_thing_not_found(db_session, primary_key, url)


if __name__ == "__main__":
    main()
