from typing import Optional

import fastapi.responses
import igsn_lib.time
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlmodel import Session
from starlette.status import HTTP_201_CREATED

from isb_lib.models.export_job import ExportJob
from isb_lib.utilities.solr_result_transformer import SolrResultTransformer, TargetExportFormat
from isb_web import isb_solr_query, analytics, sqlmodel_database
from isb_web.analytics import AnalyticsEvent
from isb_web.sqlmodel_database import SQLModelDAO

router = APIRouter(prefix="/export")
dao: Optional[SQLModelDAO] = None


def get_session():
    with dao.get_session() as session:
        yield session


def write_csv(solr_params: list[str], session: Session, export_job: ExportJob):
    export_job.tstarted = igsn_lib.time.dtnow()
    sqlmodel_database.save_or_update_export_job(session, export_job)
    response = isb_solr_query.solr_searchStream(solr_params)
    solr_response_path = f"/tmp/{export_job.uuid}_solr.json"
    with open(solr_response_path, mode="wb") as query_file:
        for chunk in response.iter_content(chunk_size=4096):
            query_file.write(chunk)
    transformed_response_path = f"/tmp/{export_job.uuid}.csv"
    solr_result_transformer = SolrResultTransformer(solr_response_path, TargetExportFormat.CSV, transformed_response_path)
    solr_result_transformer.transform()
    export_job.file_path = transformed_response_path
    export_job.tcompleted = igsn_lib.time.dtnow()
    sqlmodel_database.save_or_update_export_job(session, export_job)
    print("Finished writing query response!")


@router.get("/create")
async def export_csv(request: fastapi.Request, background_tasks: BackgroundTasks, session: Session = Depends(get_session)) -> dict:
    """Creates a new export job with the specified solr query"""
    params, properties = isb_solr_query.get_solr_params_from_request(request)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THINGS_DOWNLOAD, request, properties)
    export_job = ExportJob()
    # TODO: hook this into orcid
    export_job.creator_id = "ABCDEFG"
    sqlmodel_database.save_or_update_export_job(session, export_job)
    background_tasks.add_task(write_csv, params, session, export_job)
    status_dict = {"status": "created", "uuid": export_job.uuid}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)
