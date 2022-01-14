import logging
import typing

import fastapi
import requests
import json
import isb_web.config
from isb_web.isb_format import _NoValue
from isb_lib.core import MEDIA_JSON

ANALYTICS_URL = isb_web.config.Settings().analytics_url
ANALYTICS_DOMAIN = isb_web.config.Settings().analytics_domain


class AnalyticsEvent(_NoValue):
    """Enum class representing all possible analytics events we may record.  These need to be defined in plausible.io
    per https://plausible.io/docs/custom-event-goals#using-custom-props"""

    THING_LIST = "thing_list"
    THING_LIST_METADATA = "thing_list_metadata"
    THING_LIST_TYPES = "thing_list_types"
    THING_SOLR_SELECT = "thing_solr_select"
    THING_SOLR_LUKE_INFO = "thing_solr_luke_info"
    THING_SOLR_STREAM = "thing_solr_stream"
    THING_BY_IDENTIFIER = "thing_by_identifier"
    STAC_ITEM_BY_IDENTIFIER = "stac_item_by_identifier"
    STAC_COLLECTION = "stac_collection"
    RELATED_METADATA = "related_metadata"
    RELATED_SOLR = "related_solr"


def record_analytics_event(
    event: AnalyticsEvent,
    request: fastapi.Request,
    properties: typing.Optional[typing.Dict] = None,
) -> bool:
    """
    Records an analytics event in plausible.io
    Args:
        event: The event to record
        request: The fastapi.Request object containing all the caller info
        properties: Custom event properties

    Returns: true if plausible responds with a 202, false otherwise

    """
    logging.error(f"Request headers are {request.headers}")
    logging.error(f"Request client is {request.client}")
    logging.error(f"Request url is {request.url}")
    headers = {
        "Content-Type": MEDIA_JSON,
        "User-Agent": request.headers.get("user-agent", "no-user-agent"),
        "X-Forwarded-For": request.headers.get("x-forwarded-for", "no-client-ip")
    }
    logging.error(f"Request url is {request.url}")
    logging.error(f"Analytics request headers would be {headers}")
    referer = request.headers.get("referer")
    data_dict = {"name": event.value, "domain": ANALYTICS_DOMAIN, "url": str(request.url)}
    if referer is not None:
        data_dict["referrer"] = referer
    if properties is not None:
        # plausible.io has a bug where it needs the props to be stringified when posted in the data
        # https://github.com/plausible/analytics/discussions/1570
        data_dict["props"] = json.dumps(properties)
    post_data_str = json.dumps(data_dict).encode("utf-8")
    logging.error(f"Would be posting {post_data_str}")
    if ANALYTICS_URL == "UNSET":
        logging.error("Analytics URL is not configured.  Please check isb_web_config.env.")
        return False
    response = requests.post(ANALYTICS_URL, headers=headers, data=post_data_str)
    if response.status_code != 202:
        logging.error("Error recording analytics event %s, status code: %s", event.value, response.status_code)
    return response.status_code == 202
