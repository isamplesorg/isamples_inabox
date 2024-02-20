import datetime
from unittest.mock import patch, MagicMock

import pytest
from starlette.testclient import TestClient

from isb_lib.models.export_job import ExportJob
from isb_web.main import app


@pytest.fixture(name="client")
def client_fixture():
    # def get_repository_override():
    #     return _mock_repository()
    # app.dependency_overrides[get_repository] = get_repository_override
    client = TestClient(app)
    yield client
    # app.dependency_overrides.clear()


@patch("isb_web.export.write_csv")
@patch("isb_web.sqlmodel_database.save_or_update_export_job")
def test_export_create(mock_solr_query: MagicMock, mock_database: MagicMock, client: TestClient):
    response = client.get("/export/create")
    assert mock_solr_query.called
    assert mock_database.called
    assert response.status_code == 201


@patch("isb_web.sqlmodel_database.export_job_with_uuid")
def test_export_status(mock_database: MagicMock, client: TestClient):
    mock_database.return_value = ExportJob()
    response = client.get("/export/status?uuid=123456")
    assert response.status_code == 201

@patch("isb_web.sqlmodel_database.export_job_with_uuid")
def test_export_status_completed(mock_database: MagicMock, client: TestClient):
    job = ExportJob()
    job.tcompleted = datetime.datetime.now()
    mock_database.return_value = job
    response = client.get("/export/status?uuid=123456")
    assert response.status_code == 200


@patch("isb_web.sqlmodel_database.export_job_with_uuid")
def test_export_statu_not_found(mock_database: MagicMock, client: TestClient):
    mock_database.return_value = None
    response = client.get("/export/status?uuid=123456")
    assert response.status_code == 404
