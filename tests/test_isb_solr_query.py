from isb_web.isb_solr_query import _solr_heatmap_geom_params_str, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON, \
    replace_param_value, read_param_value


def test_solr_heat_geom_params_str():
    bb = {MIN_LAT: -90.0, MAX_LAT: 90.0, MIN_LON: -180.0, MAX_LON: 180.0}
    params_str = _solr_heatmap_geom_params_str(bb)
    assert "[-180.0 -90.0 TO 180.0 90.0]" == params_str


def test_replace_parameter_value():
    params = [['fq', 'source:*'], ['wt', 'json'], ['q', '*:*'], ['fl', ['id', 'authorizedBy', 'compliesWith',
                                                               'producedBy_samplingSite_location_longitude',
                                                               'producedBy_samplingSite_location_latitude',
                                                               'relatedResource_isb_core_id', 'curation_responsibility',
                                                               'curation_location', 'curation_accessContraints',
                                                               'curation_description', 'curation_label',
                                                               'samplingPurpose', 'registrant',
                                                               'producedBy_samplingSite_placeName',
                                                               'producedBy_samplingSite_location_elevationInMeters',
                                                               'producedBy_samplingSite_label',
                                                               'producedBy_samplingSite_description',
                                                               'producedBy_resultTime', 'producedBy_responsibility',
                                                               'producedBy_hasFeatureOfInterest',
                                                               'producedBy_description', 'producedBy_label',
                                                               'producedBy_isb_core_id', 'informalClassification',
                                                               'keywords', 'hasSpecimenCategory', 'hasMaterialCategory',
                                                               'hasContextCategory', 'description', 'label', 'source']],
     ['rows', 50000], ['start', 0], ['cursorMark', '*'], ['sort', 'id asc']]
    start_key = "start"
    start_value = 10000
    replaced_params = replace_param_value(params, {start_key: start_value})
    replaced_start_value = read_param_value(replaced_params, start_key)
    assert replaced_start_value == start_value