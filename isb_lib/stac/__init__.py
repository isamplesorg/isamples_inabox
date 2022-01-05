import typing
import geojson
import os.path

from isb_lib.core import MEDIA_GEO_JSON, MEDIA_JSON


def stac_item_from_solr_dict(
    solr_dict: typing.Dict, stac_url_prefix: str, thing_url_prefix: str
) -> typing.Optional[typing.Dict]:
    latitude = solr_dict.get("producedBy_samplingSite_location_latitude")
    longitude = solr_dict.get("producedBy_samplingSite_location_longitude")
    if latitude is None or longitude is None:
        # Don't have location, can't make a stac item
        return None

    point = geojson.Point([longitude, latitude])
    identifier = solr_dict.get("id")
    stac_item = {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": identifier,
        "geometry": point,
        "bbox": [longitude, longitude, latitude, latitude],
    }
    result_time = solr_dict.get("producedBy_resultTime")

    if result_time is None:
        # don't have a creation date in the document, pull from the source timestamp instead
        result_time = solr_dict.get("sourceUpdatedTime")
    stac_item["properties"] = {"datetime": result_time}

    links_list = [
        {
            "rel": "self",
            "href": os.path.join(stac_url_prefix, identifier),
            "type": MEDIA_GEO_JSON,
        }
    ]
    stac_item["links"] = links_list

    assets_dict = {
        "data": {
            "href": os.path.join(thing_url_prefix, identifier),
            "type": MEDIA_JSON,
            "roles": ["data"],
        }
    }
    stac_item["assets"] = assets_dict

    return stac_item
