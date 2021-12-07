import click
import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging


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