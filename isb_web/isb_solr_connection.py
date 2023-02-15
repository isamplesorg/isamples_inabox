import urllib
import requests


# Adapted from https://github.com/solrcloudpy/solrcloudpy/blob/master/solrcloudpy/connection.py
class SolrConnection:
    # base_url will come into us something like this: http://localhost:8984/solr/isb_core_records/
    def __init__(self, base_url: str, detect_nodes: bool = True):
        self.base_url = base_url
        if detect_nodes:
            self.servers = self._detect_nodes()

    def _zk_url_from_base_url(self) -> str:
        parsed_url = urllib.parse.urlparse(self.base_url)
        # "solr/isb_core_records/" -- save this for later as we'll need to reconstitue it when we get the hosts
        self.solr_path = parsed_url.path
        # "solr/isb_core_records"
        trimmed_path = self.solr_path.rstrip("/")
        # "solr"
        path_components = trimmed_path.split("/")[:-1]
        path_components.append("admin")
        path_components.append("zookeeper")
        zk_path = "/".join(path_components)
        parsed_url = parsed_url._replace(path=zk_path)
        return parsed_url.geturl()

    def _detect_nodes(self):
        self.zk_url = self._zk_url_from_base_url()
        live_nodes_url = self.zk_url + "?detail=true&path=%2Flive_nodes"
        response = requests.get(live_nodes_url)
        if response.status_code != 200:
            raise ValueError(
                "Unable to get live nodes in zookeeeper cluster, status: %s; reason: %s",
                response.status_code,
                response.reason,
            )
        json = response.json()
        children = [d["text"] for d in json["tree"][0]["children"]]
        # e.g. 192.168.128.2:8983_solr
        nodes = [c.replace("_solr", "") for c in children]
        return [f"http://{node}{self.solr_path}" for node in nodes]