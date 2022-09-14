import os.path
import logging
from isb_web import config

from isamples_metadata.Transformer import Transformer
from scripts.taxonomy.model import Model, get_model
from scripts.taxonomy.classification import get_classification_result
from scripts.taxonomy.SESARClassifierInput import SESARClassifierInput
from scripts.taxonomy.OpenContextClassifierInput import OpenContextClassifierInput

_SESAR_MATERIAL_MODEL = None
_OPENCONTEXT_MATERIAL_MODEL = None
_OPENCONTEXT_SAMPLE_MODEL = None

def set_model(collection, label_type):
    """
        Set the pretrained models by loading them from the file system
        Prerequisite: In order to use this, make sure that there is a pydantic settings file on the
        at the root of this repository named "isamples_web_config.env" with at least these variables set:

        SESAR_MATERIAL_MODEL_PATH = "/absolute/path/to/model"
        OPENCONTEXT_MATERIAL_MODEL_PATH = "/absolute/path/to/model"
        OPENCONTEXT_SAMPLE_MODEL_PATH = "/absolute/path/to/model"

        :param collection : the collection type of the sample
        :param label_type : the field that we want to predict 
    """
    if collection == "SESAR":
        _MODEL_PATH = config.Settings().sesar_material_model_path
    elif collection == "OPENCONTEXT" and label_type == "material":
        _MODEL_PATH = config.Settings().opencontext_material_model_path
    elif collection == "OPENCONTEXT" and label_type == "sample":
        _MODEL_PATH = config.Settings().opencontext_sample_model_path
    
    if not os.path.exists(_MODEL_PATH):
        logging.error(
            "Unable to locate model at path %s.  All predictions will return NOT_PROVIDED.",
            _MODEL_PATH,
        )
    elif collection == "SESAR":
        _SESAR_MATERIAL_MODEL = get_model(_MODEL_PATH)
    elif collection == "OPENCONTEXT" and label_type == "material":
        _OPENCONTEXT_MATERIAL_MODEL = get_model(_MODEL_PATH)
    elif collection == "OPENCONTEXT" and label_type == "sample":
        _OPENCONTEXT_SAMPLE_MODEL = get_model(_MODEL_PATH)

# load and initialize the models 
set_model("SESAR", "material")
set_model("OPENCONTEXT", "material")
set_model("OPENCONTEXT", "sample")


class SESARPredictor:

    def __init__(self, name: str, model: Model):
        self._name = name
        self._model = model
        self._model_valid = model is not None
        self._description_map = None

    def predict_material_type(
        self, source_record : dict
    ) -> str:
        """
        Invoke the pre-trained BERT model to predict the material type label for the specified string inputs.

        :param source_record: the raw source of a record
        :return: iSamples CV that corresponds to the label that is the prediction result of the field
        """
        if not self._model_valid:
            logging.error(
                "Returning Transformer.NOT_PROVIDED since we couldn't load the model at path %s.",
                    config.Settings().sesar_material_model_path
            )
            return Transformer.NOT_PROVIDED

        # extract the data that the model requires for classification
        sesar_input = SESARClassifierInput(source_record)
        sesar_input.parse_thing()
        self._description_map = sesar_input.get_description_map()
        # get the input string for prediction
        input_string = sesar_input.get_material_text()

        # get the prediction result with necessary fields provided
        raw_predict, raw_prob = get_classification_result(
            self._model, self.description_map, input_string, "SESAR", "material"
        )

        return SESARClassifierInput.source_to_CV[raw_predict]


class OpenContextPredictor:
    
    def __init__(self, name: str, model: Model):
        self._name = name
        self._model = model
        self._model_valid = model is not None
        self._description_map = None

    # TODO : find the field that invokes the predictor function
    # in the OpenContextTransformer
    def predict_material_type(
        self, source_record : dict
    ) -> str:
        """
        Invoke the pre-trained BERT model to predict the material type label for the specified string inputs.

        :param source_record: the raw source of a record
        :return: String label that is the prediction result of the field
        """
        if not self._model_valid:
            logging.error(
                "Returning Transformer.NOT_PROVIDED since we couldn't load the model at path %s.",
                config.Settings().opencontext_material_model_path
            )
            return Transformer.NOT_PROVIDED

        # extract the data that the model requires for classification
        oc_input = OpenContextClassifierInput(source_record)
        oc_input.parse_thing()
        self.description_map = oc_input.get_description_map()
        input_string = oc_input.get_material_text()

        # get the prediction result with necessary fields provided
        raw_predict, raw_prob = get_classification_result(
            self._model, self.description_map, input_string, "OPENCONTEXT", "material"
        )

        # TODO: return the OpenContext CV mapping that corresponds to the raw prediction
        return raw_predict
    
    def predict_sample_type(
        self, source_record : dict
    ) -> str:
        """
        Invoke the pre-trained BERT model to predict the sample type label for the specified string inputs.

        :param source_record: the raw source of a record
        :return: String label that is the prediction result of the field
        """
        if not self._model_valid:
            logging.error(
                "Returning Transformer.NOT_PROVIDED since we couldn't load the model at path %s.",
                config.Settings().opencontext_sample_model_path
            )
            return Transformer.NOT_PROVIDED

        # extract the data that the model requires for classification
        oc_input = OpenContextClassifierInput(source_record)
        oc_input.parse_thing()
        self.description_map = oc_input.get_description_map()
        input_string = oc_input.get_sample_text()

        # get the prediction result with necessary fields provided
        raw_predict, raw_prob = get_classification_result(
            self._model, self.description_map, input_string, "OPENCONTEXT", "sample"
        )

        # TODO: return the OpenContext CV mapping that corresponds to the raw prediction
        return raw_predict



SESAR_MATERIAL_PREDICTOR = SESARPredictor("Sesar_material", _SESAR_MATERIAL_MODEL)
OPENCONTEXT_MATERIAL_PREDICTOR = SESARPredictor("OpenContext_material", _OPENCONTEXT_MATERIAL_MODEL)
OPENCONTEXT_SAMPLE_PREDICTOR = SESARPredictor("OpenContext_sample", _OPENCONTEXT_SAMPLE_MODEL)
