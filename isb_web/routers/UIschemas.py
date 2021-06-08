from pydantic import BaseModel

class Url(BaseModel):
    url: str

class report(BaseModel):
    title: str
    report: str = ""