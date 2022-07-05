import logging

from starlette.datastructures import URL
from urllib import parse
from urllib.parse import urljoin


def last_path_component(url: URL) -> str:
    path_component = url.path.split("/")[-1]
    return path_component


def joined_url(original_url: str, fragment_to_join: str) -> str:
    logging.debug(f"url to join is {original_url}")
    original_url = parse.urlparse(original_url)
    logging.debug(f"url path is {original_url.path}")
    base = original_url.scheme + "://" + original_url.netloc
    joined = urljoin(base, fragment_to_join)
    logging.debug(f"joined url is {joined}")
    return joined