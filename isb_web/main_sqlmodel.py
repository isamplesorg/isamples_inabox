import fastapi
import logging
import uvicorn
from fastapi import Depends
from fastapi.logger import logger as fastapi_logger
from sqlmodel import Session
from isb_web.schemas import ThingPage
from isb_web import isb_format
from isb_web import sqlmodel_database
import typing
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.GEOMETransformer import GEOMETransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer
from isb_web.sqlmodel_database import SQLModelDAO

app = fastapi.FastAPI()
dao = SQLModelDAO()


@app.on_event("startup")
def on_startup():
    dao.connect_sqlmodel()


def get_session():
    with dao.get_session() as session:
        yield session


@app.get("/thing/", response_model=ThingPage)
def read_things(
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    status: int = 200,
    authority: str = fastapi.Query(None),
    session: Session = Depends(get_session),
):
    total_records, npages, things = sqlmodel_database.read_things(
        session, offset, limit, status, authority
    )

    params = {
        "limit": limit,
        "offset": offset,
        "status": status,
        "authority": authority,
    }
    return {
        "params": params,
        "last_page": npages,
        "total_records": total_records,
        "data": things,
    }


@app.get("/thing/{identifier:path}", response_model=typing.Any)
async def get_thing(
    identifier: str,
    full: bool = False,
    format: isb_format.ISBFormat = isb_format.ISBFormat.ORIGINAL,
    session: Session = Depends(get_session),
):
    """Record for the specified identifier"""
    item = sqlmodel_database.get_thing(session, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    if full or format == isb_format.ISBFormat.FULL:
        return item
    if format == isb_format.ISBFormat.CORE:
        authority_id = item.authority_id
        if authority_id == "SESAR":
            content = SESARTransformer(item.resolved_content).transform()
        elif authority_id == "GEOME":
            content = GEOMETransformer(item.resolved_content).transform()
        elif authority_id == "OPENCONTEXT":
            content = OpenContextTransformer(item.resolved_content).transform()
        elif authority_id == "SMITHSONIAN":
            content = SmithsonianTransformer(item.resolved_content).transform()
        else:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Core format not available for authority_id: {authority_id}",
            )
    else:
        content = item.resolved_content
    return fastapi.responses.JSONResponse(
        content=content, media_type=item.resolved_media_type
    )


def main():
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run("isb_web.main_sqlmodel:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    handler = (
        logging.StreamHandler()
    )  # RotatingFileHandler('/log/abc.log', backupCount=0)
    logging.getLogger().setLevel(logging.NOTSET)
    fastapi_logger.addHandler(handler)
    handler.setFormatter(formatter)

    fastapi_logger.info("****************** Starting Server *****************")
    main()
