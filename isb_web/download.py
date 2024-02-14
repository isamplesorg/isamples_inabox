import fastapi.responses
from fastapi import APIRouter, BackgroundTasks
from starlette.status import HTTP_201_CREATED

router = APIRouter(prefix="/download")


def write_csv(request: fastapi.Request):
    with open("/tmp/query.txt", mode="w") as query_file:
        query_file.write("foobarbaz")


@router.get("/")
async def download_csv(request: fastapi.Request, background_tasks: BackgroundTasks) -> dict:
    background_tasks.add_task(write_csv, request)
    status_dict = {"message": "data generation started"}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)