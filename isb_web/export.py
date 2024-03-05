from typing import Optional
import concurrent
import fastapi.responses
import igsn_lib.time
import json_stream.requests
import petl
from fastapi import Depends, FastAPI
from sqlmodel import Session
from starlette.responses import JSONResponse, FileResponse
from starlette.status import HTTP_201_CREATED, HTTP_404_NOT_FOUND, HTTP_200_OK, HTTP_202_ACCEPTED

from isamples_metadata.solr_field_constants import SOLR_ID, SOLR_LABEL, SOLR_HAS_CONTEXT_CATEGORY, \
    SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_SPECIMEN_CATEGORY, SOLR_KEYWORDS, SOLR_INFORMAL_CLASSIFICATION, \
    SOLR_REGISTRANT, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_RESULT_TIME
from isb_lib.models.export_job import ExportJob
from isb_lib.utilities.solr_result_transformer import SolrResultTransformer, TargetExportFormat
from isb_web import isb_solr_query, analytics, sqlmodel_database, auth
from isb_web.analytics import AnalyticsEvent
from isb_web.sqlmodel_database import SQLModelDAO

EXPORT_PREFIX = "/export"
export_app = FastAPI(prefix=EXPORT_PREFIX)
auth.add_auth_middleware_to_app(export_app)
dao: Optional[SQLModelDAO] = None
DEFAULT_SOLR_FIELDS_FOR_EXPORT = [SOLR_ID, SOLR_LABEL, SOLR_HAS_CONTEXT_CATEGORY, SOLR_HAS_MATERIAL_CATEGORY,
                                  SOLR_HAS_SPECIMEN_CATEGORY, SOLR_KEYWORDS, SOLR_INFORMAL_CLASSIFICATION,
                                  SOLR_REGISTRANT, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_DESCRIPTION,
                                  SOLR_PRODUCED_BY_RESULT_TIME]


def get_session():
    with dao.get_session() as session:
        yield session


executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)


def search_solr_and_export_results(export_job_id: str):
    """Task function that gets a queued export job from the db, executes the solr query, and writes results to disk"""

    # note that we don't seem to be able to work with the session generator on the background thread, so explicitly
    # open and close a new session for each task we execute
    with dao.get_session() as session:  # type: ignore
        export_job = sqlmodel_database.export_job_with_uuid(session, export_job_id)
        if export_job is not None:
            export_job.tstarted = igsn_lib.time.dtnow()
            sqlmodel_database.save_or_update_export_job(session, export_job)
            response = isb_solr_query.solr_searchStream(export_job.solr_query_params)  # type: ignore
            data = json_stream.requests.load(response)
            # TODO: what directory should we use?
            docs = data["result-set"]["docs"]
            generator_docs = (json_stream.to_standard_types(doc) for doc in docs)
            table = petl.fromdicts(generator_docs)
            transformed_response_path = f"/tmp/{export_job.uuid}"
            solr_result_transformer = SolrResultTransformer(table, TargetExportFormat[export_job.export_format], transformed_response_path)  # type: ignore
            solr_result_transformer.transform()
            export_job.file_path = transformed_response_path
            export_job.tcompleted = igsn_lib.time.dtnow()
            sqlmodel_database.save_or_update_export_job(session, export_job)
            print("Finished writing query response!")


@export_app.get("/create")
async def create(request: fastapi.Request, export_format: TargetExportFormat = TargetExportFormat.CSV,
                 session: Session = Depends(get_session)) -> JSONResponse:
    """Creates a new export job with the specified solr query"""

    # supported parameters are: q, fq, start, rows, format (right now format should be either CSV or JSON)

    # These will be inserted into the solr request if not present on the API call
    solr_api_defparams = {
        "wt": "json",
        "q": "*:*",
        "fl": DEFAULT_SOLR_FIELDS_FOR_EXPORT,
        "rows": 10000,
        "start": 0,
    }
    params, properties = isb_solr_query.get_solr_params_from_request(request, solr_api_defparams, ["q", "fq", "start", "rows", "fl"])
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THINGS_DOWNLOAD, request, properties)
    export_job = ExportJob()
    export_job.creator_id = auth.orcid_id_from_session_or_scope(request)
    export_job.solr_query_params = params
    export_job.export_format = export_format
    sqlmodel_database.save_or_update_export_job(session, export_job)
    executor.submit(search_solr_and_export_results, export_job.uuid)  # type: ignore
    status_dict = {"status": "created", "uuid": export_job.uuid}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)


def _not_found_response() -> JSONResponse:
    return fastapi.responses.JSONResponse(content={"status": "not_found"}, status_code=HTTP_404_NOT_FOUND)


@export_app.get("/status")
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


@export_app.get("/download")
def download(uuid: str = fastapi.Query(None), session: Session = Depends(get_session)):
    export_job = sqlmodel_database.export_job_with_uuid(session, uuid)
    if export_job is None:
        return _not_found_response()
    else:
        if export_job.file_path is not None:
            return FileResponse(export_job.file_path)
        else:
            return _not_found_response()
