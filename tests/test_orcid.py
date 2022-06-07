import os

import pytest
from isb_lib.authorization import orcid


# If manually testing, you can put these in to environment environment variables:
# https://stackoverflow.com/questions/42708389/how-to-set-environment-variables-in-pycharm
orcid_id = os.environ.get("TEST_ORCID_ID")
token = os.environ.get("TEST_ORCID_TOKEN")


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
    test_orcid_id = "test_orcid_id"
    auth_url = orcid._orcid_auth_url(test_orcid_id)
    assert test_orcid_id in auth_url


def test_headers():
    test_token = "test_token"
    headers = orcid._orcid_auth_headers(test_token)
    assert test_token in headers.get("Authorization")
    assert headers.get("Content-Type") is not None
