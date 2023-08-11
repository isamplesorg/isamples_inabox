import os

from starlette.datastructures import URL

from isb_lib.utilities import url_utilities


def test_last_path_component():
    last_path = url_utilities.last_path_component(URL("http://foo.bar/index.html"))
    assert "index.html" == last_path


def test_joined_url():
    joined_url = url_utilities.joined_url("https://hyde.cyverse.org", "/isamples_central/ui/#/dois")
    assert "https://hyde.cyverse.org/isamples_central/ui/#/dois" == joined_url
    joined_url = url_utilities.joined_url("https://hyde.cyverse.org/", "/isamples_central/ui/#/dois")
    assert "https://hyde.cyverse.org/isamples_central/ui/#/dois" == joined_url


def test_url_utilities_with_environ():
    os.environ["ISB_SITEMAP_PREFIX"] = "https://iscaws.isample.xyz/isamples_central/"
    joined_url = url_utilities.full_url_from_suffix("foo", "/manage/login?thing=IGSN:ODP02BWAO")
    assert joined_url == "https://iscaws.isample.xyz/isamples_central/manage/login?thing=IGSN:ODP02BWAO"
    del os.environ["ISB_SITEMAP_PREFIX"]


def test_url_utilities_no_environ():
    joined_url = url_utilities.full_url_from_suffix("http://localhost", "thingpage?thing=12345")
    assert joined_url == "http://localhost/thingpage?thing=12345"
