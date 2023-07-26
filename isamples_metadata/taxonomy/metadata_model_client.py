import json
from functools import lru_cache
from typing import Any

import requests

from isb_web import config

cache_size = config.Settings().modelserver_lru_cache_size


class PredictionResult:
    """Class that represents the prediction result"""
    def __init__(self, value: str, confidence: float):
        """
            Initialize the class values with the predicted label and probability logit value
            :param value the predicted label
            :param confidence the probability of the prediction
        """
        self.value = value
        self.confidence = confidence


class ModelServerClient:
    base_url: str
    base_headers: dict

    def __init__(self, base_url: str, base_headers: dict = {}):
        self.base_url = base_url
        self.base_headers = base_headers

    def _make_json_request(self, url: str, data_params: dict, rsession: requests.Session) -> Any:
        data_params_bytes: bytes = json.dumps(data_params).encode("utf-8")
        res = rsession.post(url, headers=self.base_headers, data=data_params_bytes)
        response_dict = res.json()
        return response_dict

    @staticmethod
    def _convert_to_prediction_result_list(result: Any) -> list[PredictionResult]:
        return [PredictionResult(prediction_dict["value"], prediction_dict["confidence"]) for prediction_dict in result]

    def _make_opencontext_request(self, source_record: dict, model_type: str, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        params: dict = {"source_record": source_record, "type": model_type}
        url = f"{self.base_url}opencontext"
        return ModelServerClient._convert_to_prediction_result_list(self._make_json_request(url, params, rsession))

    @lru_cache(maxsize=cache_size)
    def make_opencontext_material_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        return self._make_opencontext_request(source_record, "material", rsession)

    @lru_cache(maxsize=cache_size)
    def make_opencontext_sample_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        return self._make_opencontext_request(source_record, "sample", rsession)

    @lru_cache(maxsize=cache_size)
    def make_sesar_material_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        params: dict = {"source_record": source_record, "type": "material"}
        url = f"{self.base_url}sesar"
        return ModelServerClient._convert_to_prediction_result_list(self._make_json_request(url, params, rsession))

    @lru_cache(maxsize=cache_size)
    def make_smithsonian_sampled_feature_request(self, input_strs: list[str], rsession: requests.Session = requests.Session()) -> Any:
        params: dict = {"input": input_strs, "type": "context"}
        url = f"{self.base_url}smithsonian"
        return self._make_json_request(url, params, rsession)


headers = {"accept": "application/json", "User-Agent": "iSamples Integration Bot 2000"}
MODEL_SERVER_CLIENT = ModelServerClient(config.Settings().modelserver_url, headers)
