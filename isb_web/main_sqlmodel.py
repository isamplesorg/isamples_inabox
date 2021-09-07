import fastapi
import logging
import uvicorn
from fastapi import Depends
from fastapi.logger import logger as fastapi_logger
from sqlmodel import SQLModel, create_engine, Session, select
import isb_web.config
from isb_lib.models.thing import Thing
from typing import List
from isb_web.schemas import ThingPage
from isb_web import sqlmodel_database

app = fastapi.FastAPI()
database_url = isb_web.config.Settings().database_url
# For unit tests, this won't be set, but we provide an alternate in-memory url and override the engine, so don't worry
if database_url != "UNSET":
    engine = create_engine(database_url, echo=True)


@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


@app.get("/thingsqlmodel/", response_model=List[Thing])
def read_things(session: Session = Depends(get_session)):
    statement = select(Thing).limit(10)
    results = session.exec(statement)
    things = results.all()
    return things


@app.get("/thing/", response_model=ThingPage)
def read_things(
    session: Session = Depends(get_session),
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    status: int = 200,
    authority: str = fastapi.Query(None),
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
