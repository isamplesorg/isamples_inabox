from unittest.mock import patch, MagicMock

import pytest
from starlette.testclient import TestClient

from isb_web.main import app


@pytest.fixture(name="client")
def client_fixture():
    # def get_repository_override():
    #     return _mock_repository()
    # app.dependency_overrides[get_repository] = get_repository_override
    client = TestClient(app)
    yield client
    # app.dependency_overrides.clear()


@patch("isb_web.isb_solr_query.solr_searchStream")
def test_export(mock_solr_query: MagicMock, client: TestClient):
    response = client.get("/export/")
    assert response.status_code == 201
