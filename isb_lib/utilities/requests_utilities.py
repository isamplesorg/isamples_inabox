import logging
import random
import typing
import time
import requests
from requests import RequestException, Timeout, Response

L = logging.getLogger("requests_utilities")


class RetryingRequests:
    def __init__(self, include_random_on_failure: bool = False, timeout: int = 60, max_retries: int = 10, sleep_sec: int = 0, success_func: typing.Callable = None):
        self._include_random_on_failure = include_random_on_failure
        self._timeout = timeout
        self._max_retries = max_retries
        self._sleep_sec = sleep_sec
        self._success_func = success_func

    def get(self, url: str, rsession: requests.Session = requests.Session(), params: dict = None, headers: dict = None) -> Response:
        retries = 0
        while retries < self._max_retries:
            try:
                response = rsession.get(url, params=params, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                if self._success_func is not None:
                    result = self._success_func(response)
                    if not result:
                        raise RequestException("Invalid response according to success function")
                return response
            except (RequestException, Timeout) as e:
                L.warning(f"Request failed: {e}")
                if self._sleep_sec > 0:
                    L.warning(f"Sleeping for {self._sleep_sec} seconds before retrying…")
                    time.sleep(self._sleep_sec)
                retries += 1
                if retries < self._max_retries:
                    if self._include_random_on_failure and params is not None:
                        params["foo"] = str(random.randint(0, 10000))
                    L.warning(f"Retrying ({retries}/{self._max_retries})...")
        raise Exception(f"Failed to make a successful request after {self._max_retries} retries")

    def post(self, url, rsession: requests.Session = requests.Session(), data: dict = None, json: dict = None, headers: dict = None) -> Response:
        retries = 0
        while retries < self._max_retries:
            try:
                response = rsession.post(url, data=data, json=json, headers=headers, timeout=self._timeout)
                response.raise_for_status()
                return response
            except (RequestException, Timeout) as e:
                L.warning(f"Request failed: {e}")
                retries += 1
                if retries < self._max_retries:
                    L.warning(f"Retrying ({retries}/{self._max_retries})...")
        raise Exception(f"Failed to make a successful request after {self._max_retries} retries")
