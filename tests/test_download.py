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

def test_download(client: TestClient):
    response = client.get("/download/")
    assert response.status_code == 201
