import json
import typing
import logging
import requests
from requests import Response
from requests.auth import HTTPBasicAuth

CONTENT_TYPE = "application/vnd.api+json"
DATACITE_URL = "https://api.test.datacite.org"


def _dois_url():
    dois_url = f"{DATACITE_URL}/dois"
    return dois_url


def _dois_headers():
    headers = {"content-type": CONTENT_TYPE}
    return headers


def _dois_auth(password, username):
    auth = HTTPBasicAuth(username, password)
    return auth


def _validate_response(response) -> bool:
    if response.status_code < 200 or response.status_code >= 300:
        logging.error(
            "Error requesting new DOI, status code: %s, response %s",
            response.status_code,
            str(response.json()),
        )
        return False
    return True


def _post_to_datacite(
    rsession: requests.session, post_data_str: str, username: str, password: str
) -> Response:
    response = rsession.post(
        _dois_url(),
        headers=_dois_headers(),
        data=post_data_str,
        auth=_dois_auth(password, username),
    )
    return response


def create_draft_doi(
    rsession: requests.session,
    prefix: str,
    doi: str,
    username: str,
    password: str,
) -> typing.Optional[str]:
    attribute_dict = {}
    if doi is not None:
        attribute_dict["doi"] = doi
    else:
        attribute_dict["prefix"] = prefix
    data_dict = {"type": "dois", "attributes": attribute_dict}
    request_data = {"data": data_dict}
    post_data_str = json.dumps(request_data).encode("utf-8")
    response = _post_to_datacite(rsession, post_data_str, username, password)
    if not _validate_response(response):
        return None
    json_response = response.json()
    draft_id = json_response["data"]["id"]
    # use the DOI prefix since we creating DOIs with datacite
    return doi_from_id(draft_id)


def create_doi(
    rsession: requests.session, json_data: str, username: str, password: str
) -> bool:
    response = _post_to_datacite(rsession, json_data, username, password)
    return _validate_response(response)


def doi_from_id(raw_id: str) -> str:
    return f"doi:{raw_id}"
