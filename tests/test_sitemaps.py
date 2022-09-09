from isb_lib.sitemaps import SitemapIndexEntry, ThingSitemapIndexEntry, UrlSetEntry, ThingUrlSetEntry


def test_sitemap_index_entry():
    entry = SitemapIndexEntry("sitemap_0.xml", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_thing_sitemap_index_entry():
    entry = ThingSitemapIndexEntry("sitemap_0.xml", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_url_set_entry():
    entry = UrlSetEntry("ark:/21547/DSz2761.json", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_thing_url_set_entry():
    entry = ThingUrlSetEntry("ark:/21547/DSz2761", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None