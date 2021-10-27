import asyncio
import shutil
import typing
from aiofile import AIOFile, Writer
import click
import gzip

from isb_lib.sitemaps.thing_sitemap import (
    SitemapIndexIterator,
    UrlSetEntry,
    SitemapIndexEntry,
)

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
        header = """<?xml version="1.0" encoding="utf-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 \
http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">\n"""
        await writer(header)
        await aiodf.fsync()
        for entry in urls:
            loc_str = xmlesc(f"{BASE_HOSTNAME}/thing/{entry.identifier}?full=false&format=core")
            lastmod_str = xmlesc(entry.last_mod_str)
            url_str = f"  <url>\n    <loc>{loc_str}</loc>\n    <lastmod>{lastmod_str}</lastmod>\n  </url>\n"
            await writer(url_str)
        await aiodf.fsync()

        await writer("</urlset>")
        await aiodf.fsync()


async def write_sitemap_index_file(
    sitemap_index_entries: typing.List[SitemapIndexEntry],
):
    async with AIOFile(f"/tmp/sitemaps/sitemap-index.xml", "w") as aiodf:
        writer = Writer(aiodf)
        header = """<?xml version="1.0" encoding="utf-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 \
https://www.sitemaps.org/schemas/sitemap/0.9/siteindex.xsd">"""
        await writer(header)
        await aiodf.fsync()
        for sitemap_index_entry in sitemap_index_entries:
            loc_str = xmlesc(sitemap_index_entry.sitemap_filename)
            lastmod_str = xmlesc(sitemap_index_entry.last_mod_str)
            await writer(f"  <sitemap>\n    <loc>{loc_str}</loc>\n    <lastmod>{lastmod_str}</lastmod>\n  </sitemap>\n")
        await aiodf.fsync()

        await writer("</sitemapindex>")
        await aiodf.fsync()


@click.command()
def main():
    build_sitemap()


def build_sitemap():
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(_build_sitemap())
    loop.run_until_complete(future)


async def _build_sitemap():
    iterator = SitemapIndexIterator()
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
        with open(f"/tmp/sitemaps/{sitemap_index_entry.sitemap_filename}", "rb") as f_in:
            with gzip.open(f"/tmp/sitemaps/{sitemap_index_entry.sitemap_filename}.gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(
            "Done with urlset_iterator, wrote "
            + str(urlset_iterator.num_urls)
            + " records to "
            + sitemap_index_entry.sitemap_filename
        )
    await write_sitemap_index_file(sitemap_index_entries)


if __name__ == "__main__":
    main()
