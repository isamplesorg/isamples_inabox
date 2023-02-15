import os
import pytest
from isb_web.isb_solr_connection import SolrConnection


def test_zk_url_from_base_url():
    url = "http://localhost:8984/solr/isb_core_records/"
    parsed_url = SolrConnection(url, False)._zk_url_from_base_url()
    assert parsed_url == "http://localhost:8984/solr/admin/zookeeper"


def test_solr_url_no_detect():
    url = "http://localhost:8984/solr/isb_core_records/"
    solr_url_to_use = SolrConnection(url, False).solr_url()
    assert url == solr_url_to_use


def test_solr_url_with_choices():
    urls = ["http://foo.bar.baz", "http://foo2.bar.baz", "http://foo3.bar.baz"]
    connection = SolrConnection("http://foo.bar.baz", False)
    connection.servers = urls
    solr_url_to_use = connection.solr_url()
    assert solr_url_to_use in urls


@pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Only run this test manually, not in CI."
)
def test_live_node_parse():
    connection = SolrConnection("http://localhost:8984/solr/isb_core_records/")
    assert len(connection.servers) > 0
