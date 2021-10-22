import asyncio
import typing
from aiofile import AIOFile, Writer
import click
import click_config_file
from sqlmodel import Session

from isb_lib.sitemaps.thing_sitemap import (
    SitemapIndexIterator,
    UrlSetEntry,
    SitemapIndexEntry,
)
from isb_web.sqlmodel_database import SQLModelDAO

BASE_HOSTNAME = "https://mars.cyverse.org"

table = str.maketrans(
    {
        "<": "&lt;",
        ">": "&gt;",
        "&": "&amp;",
        "'": "&apos;",
        '"': "&quot;",
    }
)


def xmlesc(txt):
    return txt.translate(table)


# adapted from https://github.com/Haikson/sitemap-generator/blob/master/pysitemap/format_processors/xml.py
async def write_urlset_file(filename: str, urls: typing.List[UrlSetEntry]):
    async with AIOFile(f"/tmp/sitemaps/{filename}", "w") as aiodf:
        writer = Writer(aiodf)
        await writer('<?xml version="1.0" encoding="utf-8"?>\n')
        await writer(
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'
            ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
            ' xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">\n'
        )
        await aiodf.fsync()
        for entry in urls:
            loc_str = xmlesc(f"{BASE_HOSTNAME}/thing/{entry.identifier}?full=false&format=core")
            lastmod_str = xmlesc(entry.last_mod_str)
            await writer(
                f"  <url>\n    <loc>{loc_str}</loc>\n    <lastmod>{lastmod_str}</lastmod>\n  </url>\n"
            )
        await aiodf.fsync()

        await writer("</urlset>")
        await aiodf.fsync()


async def write_sitemap_index_file(
    sitemap_index_entries: typing.List[SitemapIndexEntry],
):
    async with AIOFile(f"/tmp/sitemaps/sitemap-index.xml", "w") as aiodf:
        writer = Writer(aiodf)
        await writer('<?xml version="1.0" encoding="utf-8"?>\n')
        await writer(
            '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'
            ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
            ' xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 https://www.sitemaps.org/schemas/sitemap/0.9/siteindex.xsd">\n'
        )
        await aiodf.fsync()
        for sitemap_index_entry in sitemap_index_entries:
            loc_str = xmlesc(sitemap_index_entry.sitemap_filename)
            lastmod_str = xmlesc(sitemap_index_entry.last_mod_str)
            await writer(
                f"  <sitemap>\n    <loc>{loc_str}</loc>\n    <lastmod>{lastmod_str}</lastmod>\n  </sitemap>\n"
            )
        await aiodf.fsync()

        await writer("</sitemapindex>")
        await aiodf.fsync()


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click_config_file.configuration_option(config_file_name="sitemap.cfg")
@click.pass_context
def main(ctx, db_url):
    ctx.ensure_object(dict)
    ctx.obj["db_url"] = db_url
    session = SQLModelDAO(db_url).get_session()
    build_sitemap(session)
    # for sitemap_index_entry: sitemap_index_entry:
    #     write()


def build_sitemap(session: Session):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(_build_sitemap(session))
    loop.run_until_complete(future)


async def _build_sitemap(session: Session):
    iterator = SitemapIndexIterator(session, None)
    sitemap_index_entries = []
    for urlset_iterator in iterator:
        entries_for_urlset = []
        for urlset_entry in urlset_iterator:
            entries_for_urlset.append(urlset_entry)
        sitemap_index_entry = urlset_iterator.sitemap_index_entry()
        sitemap_index_entries.append(sitemap_index_entry)
        await write_urlset_file(
            sitemap_index_entry.sitemap_filename, entries_for_urlset
        )
        print(
            "Done with urlset_iterator, wrote "
            + str(urlset_iterator.num_urls)
            + " records to "
            + sitemap_index_entry.sitemap_filename
        )
    await write_sitemap_index_file(sitemap_index_entries)


if __name__ == "__main__":
    main()
