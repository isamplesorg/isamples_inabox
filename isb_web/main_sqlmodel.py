import fastapi
import logging
import uvicorn
from fastapi.logger import logger as fastapi_logger
from sqlmodel import SQLModel, create_engine, Session, select
import isb_web.config
from isb_lib.models.thing import Thing

DATABASE_URL = isb_web.config.Settings().database_url
app = fastapi.FastAPI()
engine = create_engine(DATABASE_URL, echo=True)


@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)

@app.get("/thingsqlmodel/")
def read_things():
    with Session(engine) as session:
        statement = select(Thing).limit(10)
        results = session.exec(statement)
        things = results.all()
        return things

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