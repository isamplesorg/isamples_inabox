import pytest
import requests
import os
import lxml.etree
import random
import dateparser
import gzip
from lxml.etree import _Element


@pytest.fixture
def rsession():
    return requests.session()


@pytest.fixture
def sitemap_index_url():
    sitemap_index_url = os.getenv("INPUT_SITEMAP_INDEX_URL")
    if sitemap_index_url is None:
        sitemap_index_url = "https://mars.cyverse.org/sitemaps/sitemap-index.xml"
    return sitemap_index_url


def _assert_date_tag_text(date_tag: _Element):
    last_mod_date = dateparser.parse(date_tag.text)
    assert last_mod_date is not None


def test_sitemap(rsession: requests.sessions, sitemap_index_url: str):
    """
    General approach is as follows:
    (1) Hit the sitemap-index.xml, and choose one of the random sitemaps inside
    (2) Download the sitemap and unzip it
    (3) Choose 100 of the random urls in the sitemap and resolve the URL
    (4) Also, for all dates, verify that they are parseable by dateparser
    """
    res = rsession.get(sitemap_index_url)
    xmlp = lxml.etree.XMLParser(
        recover=True,
        remove_comments=True,
        resolve_entities=False,
    )
    root = lxml.etree.fromstring(res.content, parser=xmlp)
    sitemap_list = root.getchildren()
    """These sitemap children look like this:
          <sitemap>
            <loc>http://mars.cyverse.org/sitemaps/sitemap-5.xml.gz</loc>
            <lastmod>2006-08-10T12:00:00Z</lastmod>
          </sitemap>    
    """
    random_child_index = random.randrange(0, len(sitemap_list) - 1)
    random_child = sitemap_list[random_child_index]
    random_child_children = random_child.getchildren()
    for child_element in random_child_children:
        if "lastmod" in child_element.tag:
            _assert_date_tag_text(child_element)
        elif "loc" in child_element.tag:
            sitemap_file_loc = child_element.text
            res = rsession.get(sitemap_file_loc)
            uncompressed_text = gzip.decompress(res.content)
            sitemap_file_root = lxml.etree.fromstring(uncompressed_text, parser=xmlp)
            urlset_children = sitemap_file_root.getchildren()
            """The urlset children look like this:
                  <url>
                    <loc>http://mars.cyverse.org/thing/IGSN:NEON01IJE?full=false&amp;format=core</loc>
                    <lastmod>2020-03-14T10:07:56Z</lastmod>
                  </url>            
            """

            # Get 100 randomly selected urlset children and try and resolve the url in the loc
            for n in range(0, 100):
                random_url_tag = urlset_children[
                    random.randrange(0, len(urlset_children))
                ]
                for url_child in random_url_tag.getchildren():
                    if "lastmod" in url_child.tag:
                        _assert_date_tag_text(url_child)
                    elif "loc" in url_child.tag:
                        loc = url_child.text
                        # Open a request to this URL and verify we get some valid JSON back
                        url_child_res = rsession.get(loc)
                        url_child_json = url_child_res.json()
                        assert url_child_json is not None
                        # also verify it has something reasonable, like our sampleidentifier
                        assert url_child_json.get("sampleidentifier") is not None
