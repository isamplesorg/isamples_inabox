import logging
import os
from starlette.datastructures import URL
from urllib import parse
from urllib.parse import urljoin


def last_path_component(url: URL) -> str:
    path_component = url.path.split("/")[-1]
    return path_component


def joined_url(original_url: str, fragment_to_join: str) -> str:
    logging.debug(f"url to join is {original_url}")
    parse_result = parse.urlparse(original_url)
    logging.debug(f"url path is {parse_result.path}")
    base = parse_result.scheme + "://" + parse_result.netloc
    joined = urljoin(base, fragment_to_join)
    logging.debug(f"joined url is {joined}")
    return joined


# When we run in Docker, we get the full URL passed in via a container environment variable.  If the environment
# variable is present, we'll join that to the postfix.  If not, we'll join it to the request url.
def full_url_from_suffix(request_url: str, suffix: str) -> str:
    base_url = request_url
    # If we're running in the Docker container, this will be defined as part of docker-compose.yml
    if "ISB_SITEMAP_PREFIX" in os.environ:
        base_url = os.environ["ISB_SITEMAP_PREFIX"]
    if not suffix.startswith("/"):
        suffix = f"/{suffix}"
    if base_url.endswith("/"):
        base_url = base_url[:-1]
    return f"{base_url}{suffix}"
