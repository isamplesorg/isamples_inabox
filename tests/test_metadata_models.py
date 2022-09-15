import asyncio
import datetime
import json
import os.path
import tempfile

import lxml
import pytest
import requests

from isamples_metadata.metadata_models import (
    SESAR_MATERIAL_PREDICTOR,
    OPENCONTEXT_MATERIAL_PREDICTOR,
    OPENCONTEXT_SAMPLE_PREDICTOR
) 

def test_sesar_material_model():
    sesar_source_path = [ "./test_data/SESAR/raw/EOI00002Hjson-ld.json" ]
    with open(sesar_source_path) as source_file:
        source_record = json.load(source_file)
        prediction = SESAR_MATERIAL_PREDICTOR.predict_material_type(source_record)
        assert prediction is not None
        # TODO : check if prediction result is correct ? 


def test_opencontext_material_model():
    oc_source_path = [ "./test_data/OpenContext/raw/ark-28722-k2b570022.json" ]
    with open(oc_source_path) as source_file:
        source_record = json.load(source_file)
        prediction = OPENCONTEXT_MATERIAL_PREDICTOR.predict_material_type(source_record)
        assert prediction is not None
        # TODO : check if prediction result is correct ? 


def test_opencontext_sample_model():
    oc_source_path = [ "./test_data/OpenContext/raw/ark-28722-k2b570022.json" ]
    with open(oc_source_path) as source_file:
        source_record = json.load(source_file)
        prediction = OPENCONTEXT_SAMPLE_PREDICTOR.predict_sample_type(source_record)
        assert prediction is not None
        # TODO : check if prediction result is correct ? 
