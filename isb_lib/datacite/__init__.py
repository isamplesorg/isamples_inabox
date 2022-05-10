import json
import typing
import logging
import requests
from requests.auth import HTTPBasicAuth

import isb_web.config

CONTENT_TYPE = "application/vnd.api+json"
DATACITE_URL = "https://api.test.datacite.org"


def create_draft_doi(
    rsession: requests.session,
    prefix: str,
    username: str,
    password: str,
) -> typing.Optional[str]:
    dois_url = f"{DATACITE_URL}/dois"
    headers = {"content-type": CONTENT_TYPE}
    request_data = {
        "data": {
            "type": "dois",
            "attributes": {
                "prefix": prefix
            }
        },
    }
    post_data_str = json.dumps(request_data).encode("utf-8")
    auth = HTTPBasicAuth(username, password)
    response = rsession.post(
        dois_url, headers=headers, data=post_data_str, auth=auth
    )
    if response.status_code < 200 or response.status_code >= 300:
        logging.error(
            "Error requesting new DOI, status code: %s, response %s",
            response.status_code,
            str(response.json())
        )
        return None
    json_response = response.json()
    return json_response["data"]["id"]
