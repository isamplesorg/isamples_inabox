from typing import Optional
import concurrent
import fastapi.responses
import igsn_lib.time
from fastapi import APIRouter, Depends
from sqlmodel import Session
from starlette.responses import JSONResponse, FileResponse
from starlette.status import HTTP_201_CREATED, HTTP_404_NOT_FOUND, HTTP_200_OK, HTTP_202_ACCEPTED

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


executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)


def search_solr_and_export_results(export_job_id: str):
    """Task function that gets a queued export job from the db, executes the solr query, and writes results to disk"""

    # note that we don't seem to be able to work with the generator on the background thread, so explicitly
    # open and close a new session for each task we execute
    with dao.get_session() as session: # type: ignore
        export_job = sqlmodel_database.export_job_with_uuid(session, export_job_id)
        if export_job is not None:
            export_job.tstarted = igsn_lib.time.dtnow()
            sqlmodel_database.save_or_update_export_job(session, export_job)
            response = isb_solr_query.solr_searchStream(export_job.solr_query_params) # type: ignore
            # TODO: what directory should we use?
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
async def create(request: fastapi.Request, session: Session = Depends(get_session)) -> JSONResponse:
    """Creates a new export job with the specified solr query"""
    params, properties = isb_solr_query.get_solr_params_from_request(request)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THINGS_DOWNLOAD, request, properties)
    export_job = ExportJob()
    # TODO: hook this into orcid
    export_job.creator_id = "ABCDEFG"
    export_job.solr_query_params = params
    sqlmodel_database.save_or_update_export_job(session, export_job)
    executor.submit(search_solr_and_export_results, export_job.uuid) # type: ignore
    status_dict = {"status": "created", "uuid": export_job.uuid}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)


def _not_found_response() -> JSONResponse:
    return fastapi.responses.JSONResponse(content={"status": "not_found"}, status_code=HTTP_404_NOT_FOUND)


@router.get("/status")
def status(uuid: str = fastapi.Query(None), session: Session = Depends(get_session)) -> JSONResponse:
    """Looks up the status of the export job with the specified uuid"""
    export_job = sqlmodel_database.export_job_with_uuid(session, uuid)
    if export_job is None:
        return _not_found_response()
    else:
        if export_job.tcompleted is not None:
            content = {"status": "completed", "tcompleted": str(export_job.tcompleted)}
            return fastapi.responses.JSONResponse(content=content, status_code=HTTP_200_OK)
        elif export_job.tstarted is not None:
            content = {"status": "started", "tstarted": str(export_job.tstarted)}
            return fastapi.responses.JSONResponse(content=content, status_code=HTTP_202_ACCEPTED)
        else:
            return fastapi.responses.JSONResponse(content={"status": "created"}, status_code=HTTP_201_CREATED)


@router.get("/download")
def download(uuid: str = fastapi.Query(None), session: Session = Depends(get_session)):
    export_job = sqlmodel_database.export_job_with_uuid(session, uuid)
    if export_job is None:
        return _not_found_response()
    else:
        if export_job.file_path is not None:
            return FileResponse(export_job.file_path)
        else:
            return _not_found_response()
