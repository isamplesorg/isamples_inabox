import click
import click_config_file
from isb_lib.sitemaps.thing_sitemap import SitemapIndexIterator
from isb_web.sqlmodel_database import SQLModelDAO


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
    iterator = SitemapIndexIterator(session, None)
    for urlset_iterator in iterator:
        for urlset_entry in urlset_iterator:
            print("current entry is " + urlset_entry.identifier + " " + str(urlset_entry.last_mod))
        print("Done with urlset_iterator, completed: " + str(urlset_iterator.num_urls))


if __name__ == "__main__":
    main()