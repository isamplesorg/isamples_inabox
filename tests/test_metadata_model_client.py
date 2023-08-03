from unittest.mock import patch, MagicMock

from isamples_metadata.taxonomy.metadata_model_client import MODEL_SERVER_CLIENT, PredictionResult


@patch("isamples_metadata.taxonomy.metadata_model_client.requests.session")
def test_sesar_material_model_client(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{
        "value": "foo",
        "confidence": 0.78
    }]
    mock_request.post.return_value = mock_response
    result = MODEL_SERVER_CLIENT.make_sesar_material_request({}, mock_request)
    assert result is not None
    prediction_result = result[0]
    assert type(result[0]) is PredictionResult
    assert prediction_result.value == "foo"
    assert prediction_result.confidence == 0.78