import fastapi.responses
from fastapi import APIRouter, BackgroundTasks
from starlette.status import HTTP_201_CREATED

from isb_web import isb_solr_query, analytics
from isb_web.analytics import AnalyticsEvent

router = APIRouter(prefix="/export")


def write_csv(solr_params: list[str]):
    response = isb_solr_query.solr_searchStream(solr_params)
    with open("/tmp/query_response.txt", mode="wb") as query_file:
        for chunk in response.iter_content(chunk_size=4096):
            query_file.write(chunk)
    print("Finished writing query response!")


@router.get("/")
async def export_csv(request: fastapi.Request, background_tasks: BackgroundTasks) -> dict:
    params, properties = isb_solr_query.get_solr_params_from_request(request)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THINGS_DOWNLOAD, request, properties)
    background_tasks.add_task(write_csv, params)
    status_dict = {"message": "data generation started"}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)