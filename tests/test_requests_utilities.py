from unittest.mock import patch, MagicMock

import pytest
from requests import RequestException, Timeout

from isb_lib import opencontext_adapter
from isb_lib.utilities.requests_utilities import RetryingRequests


def _successful_test_request() -> MagicMock:
    mock_request = MagicMock()
    mock_response = _success_response()
    mock_request.get.return_value = mock_response
    return mock_request


def _success_response():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "oc-api:has-results": "bar",
    }
    return mock_response


def _failing_test_request() -> MagicMock:
    mock_request = MagicMock()
    mock_request.get.side_effect = RequestException("server not responding")
    return mock_request


def test_retrying_requests():
    retrying_requests = RetryingRequests(True)
    request = _successful_test_request()
    response = retrying_requests.get("http://foo.bar", request)
    _assert_success_response(response)


def _assert_success_response(response):
    json = response.json()
    assert "bar" == json["oc-api:has-results"]


def test_retrying_requests_raises_on_failure():
    retrying_requests = RetryingRequests(True)
    request = _failing_test_request()
    with pytest.raises(Exception):
        retrying_requests.get("http://foo.bar", request)


num_requests = 0


def _first_fail_second_success_side_effect(*args, **kwargs):
    global num_requests
    num_requests = num_requests + 1
    if num_requests == 1:
        raise Timeout("server not responding")
    else:
        return _success_response()


def test_first_fail_second_success():
    global num_requests
    retrying_requests = RetryingRequests(True)
    mock_request = MagicMock()
    mock_request.get.side_effect = _first_fail_second_success_side_effect
    response = retrying_requests.get("http://foo.bar", mock_request)
    _assert_success_response(response)
    # This should be 2 because we should have failed the first time and retried
    assert num_requests == 2


num_success_func_invocations = 0


def _success_func(response) -> bool:
    global num_success_func_invocations
    num_success_func_invocations = num_success_func_invocations + 1
    return opencontext_adapter.is_valid_opencontext_response(response)


def _first_invalid_response_second_valid_side_effect(*args, **kwargs):
    global num_success_func_invocations
    if num_success_func_invocations < 1:
        mock_response = MagicMock()
        mock_response.status_code = 200
        # even with a 200 code, this empty json should be treated as invalid
        mock_response.json.return_value = {}
        return mock_response
    else:
        return _success_response()


def test_first_empty_response_second_success():
    global num_success_func_invocations
    mock_request = MagicMock()
    mock_request.get.side_effect = _first_invalid_response_second_valid_side_effect
    retrying_request = RetryingRequests(True, 60, 10, 0, _success_func)
    response = retrying_request.get("http://foo.bar", mock_request)
    _assert_success_response(response)
    assert 2 == num_success_func_invocations
