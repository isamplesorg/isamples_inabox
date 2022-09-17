import json
from isamples_metadata.metadata_models import (
    initialize_models,
    get_sesar_material_model,
    get_oc_material_model,
    get_oc_sample_model,
    SESARMaterialPredictor,
    OpenContextMaterialPredictor,
    OpenContextSamplePredictor
)


def test_sesar_material_model():
    initialize_models()
    sesar_model = get_sesar_material_model()
    # load the model predictor 
    smp = SESARMaterialPredictor(sesar_model)
    sesar_source_path = [ "./test_data/SESAR/raw/EOI00002Hjson-ld.json" ]
    with open(sesar_source_path) as source_file:
        source_record = json.load(source_file)
        prediction = smp.predict_material_type(source_record)
        assert prediction is not None
        # TODO : check if prediction result is correct ? 


def test_opencontext_material_model():
    initialize_models()
    oc_model = get_oc_material_model()
    # load the model predictor 
    ocmp = OpenContextMaterialPredictor(oc_model)
    oc_source_path = [ "./test_data/OpenContext/raw/ark-28722-k2b570022.json" ]
    with open(oc_source_path) as source_file:
        source_record = json.load(source_file)
        prediction = ocmp.predict_material_type(source_record)
        assert prediction is not None
        # TODO : check if prediction result is correct ? 


def test_opencontext_sample_model():
    initialize_models()
    oc_model = get_oc_sample_model()
    # load the model predictor 
    ocsp = OpenContextSamplePredictor(oc_model)
    oc_source_path = [ "./test_data/OpenContext/raw/ark-28722-k2b570022.json" ]
    with open(oc_source_path) as source_file:
        source_record = json.load(source_file)
        prediction = ocsp.predict_sample_type(source_record)
        assert prediction is not None
        # TODO : check if prediction result is correct ? 
