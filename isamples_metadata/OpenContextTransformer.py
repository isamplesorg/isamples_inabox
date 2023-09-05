import typing
from typing import Optional

import isamples_metadata.Transformer
from isamples_metadata.Transformer import (
    Transformer,
    AbstractCategoryMetaMapper,
    StringEqualityCategoryMapper,
    AbstractCategoryMapper,
)
from isamples_metadata.controlled_vocabularies import CONTEXT_PAST_HUMAN_ACTIVITIES
from isamples_metadata.metadata_exceptions import MissingIdentifierException
from isamples_metadata.taxonomy.metadata_model_client import MODEL_SERVER_CLIENT, PredictionResult
from isamples_metadata.vocabularies import vocabulary_mapper


class MaterialCategoryMetaMapper(AbstractCategoryMetaMapper):
    _anthropogenicMaterialMapper = StringEqualityCategoryMapper(
        [
            "Architectural Element",
            "Bulk Ceramic",
            "Glass",
        ],
        "Anthropogenic material",
    )

    _anthropogenicMetalMapper = StringEqualityCategoryMapper(
        ["Coin"],
        "Anthropogenic metal",
    )

    _biogenicMapper = StringEqualityCategoryMapper(
        ["Animal Bone", "Human Bone", "Non Diagnostic Bone", "Shell"],
        "Biogenic non organic material",
    )

    _organicMapper = StringEqualityCategoryMapper(
        ["Plant remains"],
        "Organic material",
    )

    _materialMapper = StringEqualityCategoryMapper(
        ["Biological record", "Biological subject, Ecofact"],
        "Material",
    )

    _rockMapper = StringEqualityCategoryMapper(
        ["Bulk Lithic", "Groundstone"],
        "Rock"
    )

    _naturalSolidMaterialMapper = StringEqualityCategoryMapper(
        ["Natural solid material"],
        "Natural solid material"
    )

    _mineralMapper = StringEqualityCategoryMapper(
        ["Mineral"],
        "Mineral"
    )

    _notSampleMapper = StringEqualityCategoryMapper(
        [
            "Sample, Collection, or Aggregation",
            "Human Subject",
            "Reference Collection",
        ],
        Transformer.NOT_PROVIDED
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._anthropogenicMaterialMapper,
            cls._anthropogenicMetalMapper,
            cls._biogenicMapper,
            cls._organicMapper,
            cls._rockMapper,
            cls._naturalSolidMaterialMapper,
            cls._mineralMapper,
            cls._notSampleMapper
        ]


class SpecimenCategoryMetaMapper(AbstractCategoryMetaMapper):
    _organismPartMapper = StringEqualityCategoryMapper(
        [
            "Animal Bone",
            "Human Bone",
            "Non Diagnostic Bone",
        ],
        "Organism part",
    )
    _artifactMapper = StringEqualityCategoryMapper(
        [
            "Architectural Element",
            "Bulk Ceramic",
            "Bulk Lithic",
            "Coin",
            "Glass",
            "Groundstone",
            "Pottery",
            "Sculpture",
            "Sample",
        ],
        "Artifact",
    )
    _biologicalSpecimenMapper = StringEqualityCategoryMapper(
        [
            "Biological record",
            "Biological subject, Ecofact",
            "Plant remains",
        ], "Biological specimen"
    )
    _physicalSpecimenMapper = StringEqualityCategoryMapper(
        [
            "Sample, Collection, or Aggregation",
            "Object"
        ],
        "Physical specimen"
    )
    _organismProductMapper = StringEqualityCategoryMapper(
        ["Shell"], "Organism product"
    )
    _notSampleMapper = StringEqualityCategoryMapper(
        [
            "Sample, Collection, or Aggregation",
            "Human Subject",
            "Reference Collection",
        ],
        Transformer.NOT_PROVIDED
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._organismPartMapper,
            cls._artifactMapper,
            cls._biologicalSpecimenMapper,
            cls._physicalSpecimenMapper,
            cls._organismProductMapper,
            cls._notSampleMapper
        ]


class OpenContextTransformer(Transformer):

    def __init__(self, source_record: typing.Dict):
        super().__init__(source_record)
        self._material_prediction_results: typing.Optional[list] = None
        self._specimen_prediction_results: typing.Optional[list] = None

    def _citation_uri(self) -> str:
        citation_uri = self.source_record.get("citation uri")
        if citation_uri is None:
            raise MissingIdentifierException("OpenContext record is missing a citation uri")
        return citation_uri

    def id_string(self) -> str:
        citation_uri = self._citation_uri()
        return f"metadata/{citation_uri.removeprefix(Transformer.N2T_ARK_PREFIX)}"

    def sample_identifier_string(self) -> str:
        return self._citation_uri().removeprefix(Transformer.N2T_PREFIX)

    def sample_label(self) -> str:
        label = self.source_record.get("label", Transformer.NOT_PROVIDED)
        item_category = self._item_category()
        if len(item_category) > 0:
            label = f"{item_category} {label}"
        return label

    def sample_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label(
            "early bce/ce", self.source_record, description_pieces, "'early bce/ce'"
        )
        self._transform_key_to_label(
            "late bce/ce", self.source_record, description_pieces, "'late bce/ce'"
        )
        self._transform_key_to_label("updated", self.source_record, description_pieces, "'updated'")
        for consists_of_str in self.source_record.get("Consists of", []):
            self._transform_key_to_label_str(
                self._get_oc_str_or_dict_item_label(consists_of_str), description_pieces, "'Consists of'"
            )
        for has_type_str in self.source_record.get("Has type", []):
            self._transform_key_to_label_str(
                self._get_oc_str_or_dict_item_label(has_type_str), description_pieces, "'Has type'"
            )
        for has_anatomical_str in self.source_record.get(
            "Has anatomical identification", []
        ):
            self._transform_key_to_label_str(
                self._get_oc_str_or_dict_item_label(has_anatomical_str),
                description_pieces,
                "'Has anatomical identification'",
            )
        for temporal_coverage_str in self.source_record.get("Temporal Coverage", []):
            self._transform_key_to_label_str(
                self._get_oc_str_or_dict_item_label(temporal_coverage_str),
                description_pieces,
                "'Temporal coverage'",
            )
        return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)

    def _get_oc_str_or_dict_item_label(self, str_or_dict):
        """A utility method to get a dictionary label or if a string, return the string"""
        # This is a bit messy, but it should be a bit forgiving if the OC API returns
        # dict or string items for certain record attributes.
        if isinstance(str_or_dict, dict):
            # this item is a dictionary.
            return str_or_dict.get("label")
        elif isinstance(str_or_dict, str):
            return str_or_dict
        return str_or_dict

    def _material_type(self) -> typing.Optional[str]:
        for consists_of_dict in self.source_record.get("Consists of", []):
            return self._get_oc_str_or_dict_item_label(consists_of_dict)
        return None

    def _specimen_type(self) -> typing.Optional[str]:
        for has_type_dict in self.source_record.get("Has type", []):
            return self._get_oc_str_or_dict_item_label(has_type_dict)
        return None

    def sample_registrant(self) -> str:
        return ""

    def sample_sampling_purpose(self) -> str:
        return ""

    def has_context_categories(self) -> typing.List[str]:
        return [vocabulary_mapper.SAMPLED_FEATURE.term_for_key("pasthumanoccupationsite").metadata_dict()]

    def _compute_material_prediction_results(self) -> typing.Optional[typing.List[PredictionResult]]:
        item_category = self._item_category()
        to_classify_items = ["Object", "Pottery", "Sample", "Sculpture"]
        if item_category not in to_classify_items:
            # Have specified mapping, won't predict
            return None
        elif self._material_prediction_results is not None:
            # Have already computed, don't predict again
            return self._material_prediction_results
        else:
            self._material_prediction_results = MODEL_SERVER_CLIENT.make_opencontext_material_request(self.source_record)
            return self._material_prediction_results

    def _item_category(self):
        return self.source_record.get("item category", "")

    def has_material_categories(self) -> typing.List[str]:
        item_category = self._item_category()
        to_classify_items = ["Object", "Pottery", "Sample", "Sculpture"]
        if item_category in to_classify_items:
            prediction_results = self._compute_material_prediction_results()
            if prediction_results is not None:
                return [prediction.value for prediction in prediction_results]
            else:
                return []
        return MaterialCategoryMetaMapper.categories(item_category)

    def has_material_category_confidences(self, material_categories: typing.List[str]) -> typing.Optional[typing.List[float]]:
        prediction_results = self._compute_material_prediction_results()
        if prediction_results is None:
            return None
        else:
            return [prediction.confidence for prediction in prediction_results]

    def _compute_specimen_prediction_results(self) -> typing.Optional[typing.List[PredictionResult]]:
        item_category = self._item_category()
        to_classify_items = ["Animal Bone"]
        if item_category not in to_classify_items:
            # Have specified mapping, won't predict
            return None
        elif self._specimen_prediction_results is not None:
            # Have already computed, don't predict again
            return self._specimen_prediction_results
        else:
            self._specimen_prediction_results = MODEL_SERVER_CLIENT.make_opencontext_sample_request(self.source_record)
            return self._specimen_prediction_results

    def has_specimen_categories(self) -> typing.List[str]:
        item_category = self._item_category()
        to_classify_items = ["Animal Bone"]
        if item_category in to_classify_items:
            prediction_results = self._compute_specimen_prediction_results()
            if prediction_results is not None:
                return [prediction.value for prediction in prediction_results]
            else:
                return []
        return SpecimenCategoryMetaMapper.categories(item_category)

    def has_specimen_category_confidences(self, specimen_categories: typing.List[str]) -> typing.Optional[typing.List[float]]:
        prediction_results = self._compute_specimen_prediction_results()
        if prediction_results is None:
            return None
        else:
            return [prediction.confidence for prediction in prediction_results]

    def _context_label_pieces(self) -> typing.List[str]:
        context_label = self.source_record.get("context label")
        if type(context_label) is str and len(context_label) > 0:
            return context_label.split("/")
        else:
            return []

    def keywords(self) -> typing.List[str]:
        return self._context_label_pieces()

    def produced_by_id_string(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_label(self) -> str:
        return self.source_record.get("project label", Transformer.NOT_PROVIDED)

    def produced_by_description(self) -> str:
        return self.source_record.get("project href", Transformer.NOT_PROVIDED)

    def produced_by_feature_of_interest(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_responsibilities(self) -> typing.List[str]:
        # from ekansa:
        # "Creator" is typically a project PI (Principle Investigator). They may or may not be the person that
        # collected the sample. If given, a "Contributor" is the person that originally collected or first
        # described the specimen.
        responsibilities = []
        creators = self.source_record.get("Creator")
        if creators is not None:
            for creator in creators:
                responsibilities.append(f"creator: {creator}")
        contributors = self.source_record.get("Contributor")
        if contributors is not None:
            for contributor in contributors:
                responsibilities.append(f"collector: {contributor}")
        return responsibilities

    def produced_by_result_time(self) -> str:
        return self.source_record.get("published", Transformer.NOT_PROVIDED)

    def sampling_site_description(self) -> str:
        return Transformer.NOT_PROVIDED

    def sampling_site_label(self) -> str:
        return self.source_record.get("context label", Transformer.NOT_PROVIDED)

    def sampling_site_elevation(self) -> str:
        return Transformer.NOT_PROVIDED

    def sampling_site_latitude(self) -> Optional[typing.SupportsFloat]:
        return _content_latitude(self.source_record)

    def sampling_site_longitude(self) -> Optional[typing.SupportsFloat]:
        return _content_longitude(self.source_record)

    def sampling_site_place_names(self) -> typing.List:
        return self._context_label_pieces()

    def informal_classification(self) -> typing.List[str]:
        classifications = []
        for consists_of_dict in self.source_record.get("Has taxonomic identifier", []):
            classifications.append(self._get_oc_str_or_dict_item_label(consists_of_dict))
        return classifications

    def last_updated_time(self) -> Optional[str]:
        return self.source_record.get("updated", None)

    def authorized_by(self) -> typing.List[str]:
        # Don't have this information
        return []

    def complies_with(self) -> typing.List[str]:
        # Don't have this information
        return []

    def h3_function(self) -> typing.Callable:
        return geo_to_h3


def _content_latitude(content: typing.Dict) -> Optional[float]:
    return content.get("latitude", None)


def _content_longitude(content: typing.Dict) -> Optional[float]:
    return content.get("longitude", None)


def geo_to_h3(content: typing.Dict, resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> Optional[str]:
    return isamples_metadata.Transformer.geo_to_h3(_content_latitude(content), _content_longitude(content), resolution)
