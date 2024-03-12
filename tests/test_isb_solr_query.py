from isb_web.isb_solr_query import _solr_heatmap_geom_params_str, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON, \
    replace_param_value, read_param_value


def test_solr_heat_geom_params_str():
    bb = {MIN_LAT: -90.0, MAX_LAT: 90.0, MIN_LON: -180.0, MAX_LON: 180.0}
    params_str = _solr_heatmap_geom_params_str(bb)
    assert "[-180.0 -90.0 TO 180.0 90.0]" == params_str
