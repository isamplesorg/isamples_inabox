import fastapi.responses
from fastapi import APIRouter, BackgroundTasks
from starlette.status import HTTP_201_CREATED

router = APIRouter(prefix="/download")


def write_csv(query: str):
    with open("/tmp/query.txt", mode="w") as query_file:
        query_file.write(query)


@router.get("/csv")
async def download_csv(q: str, background_tasks: BackgroundTasks) -> dict:
    background_tasks.add_task(write_csv, q)
    status_dict = {"message": "data generation started"}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)