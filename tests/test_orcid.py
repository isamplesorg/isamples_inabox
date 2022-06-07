import os

import pytest
from isb_lib.authorization import orcid

orcid_id = "000-0003-2109-7692"
token = "b035c8e7-60ee-4400-a6d3-9b02ff9ff1b0"


@pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Don't run live requests in CI"
)
def test_integration():
    """Manual integration test useful for running things against a live endpoint.  Inappropriate for unit testing,
    but helpful as a smoke test to make sure all the pieces fit together.  You'll need to manually generate a token
    and orcid via the instructions here: https://orcid.github.io/orcid-api-tutorial/get/ -- The Google OAuth 2.0
    Playground route was easy to use and worked well in Chrome."""
    authorized = orcid.authorize_token_for_orcid_id(token, orcid_id)
    assert authorized


def test_url():
    auth_url = orcid._orcid_auth_url(orcid_id)
    assert orcid_id in auth_url


def test_headers():
    headers = orcid._orcid_auth_headers(token)
    assert token in headers.get("Authorization")
    assert headers.get("Content-Type") is not None
