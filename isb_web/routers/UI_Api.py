import os
import fastapi
import requests
import isb_web.routers.settings as settings
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from urllib.parse import urlencode, parse_qsl
from isb_web.routers import UIschemas
from github import Github, GithubException

LOGIN_URL = "https://github.com/login/oauth/authorize"
TOKEN_URL = "https://github.com/login/oauth/access_token"
REDIRECT_URL = f'{settings.app_url}/redirect/mainPage'
token = "no authentication"
Code = "no login"


router = APIRouter()

THIS_PATH = os.path.dirname(os.path.abspath(__file__))
router.mount(
    "/static",
    fastapi.staticfiles.StaticFiles(directory=os.path.join(THIS_PATH, "../static")),
    name="static",
)
templates = fastapi.templating.Jinja2Templates(
    directory=os.path.join(THIS_PATH, "../templates")
)

@router.get("/login", response_class=HTMLResponse, summary="redirect to login page")
async def login(request: fastapi.Request):
    return templates.TemplateResponse("login.html", {"request": request})

@router.get("/login/Git", summary="redirect to git login page")
async def gitLogin() -> UIschemas.Url:
    """
    in case the client_id show in the front side
    ,then redirect to github login page
    """
    params = {
        "client_id": settings.client_id,
        "redirect_url": REDIRECT_URL,
        "scope": "repo",
    }
    return UIschemas.Url(url = (f'{LOGIN_URL}?{urlencode(params)}'))


@router.get("/redirect/mainPage", response_class=RedirectResponse, summary="redirect page from github")
async def mainPage(code:str = None):
    """
    in case the code parameter show in the url, then redirect to main page
    """
    params = {
        'client_id': settings.client_id,
        'client_secret': settings.client_secret,
        'code': code
    }
    token_request = requests.post(TOKEN_URL, params=params)
    response: Dict[bytes, bytes] = dict(parse_qsl(token_request.content))
    github_token = response[b"access_token"].decode("utf-8")

    global Code
    global token
    token = github_token
    Code = code
    return RedirectResponse(f'{settings.app_url}/mainPage')


@router.get("/mainPage", response_class=HTMLResponse, summary="redirect to main page")
async def mainPage(request: fastapi.Request):
    return templates.TemplateResponse("Records_View.html", {"request":request})

@router.post("/issues", summary="create a github issue")
async def createIssue(body: UIschemas.report):
    try:
        git = Github(token)
        repo = git.get_repo(f"{settings.github_ower}/{settings.repository}")
        repo.create_issue(title=body.title, body=body.report)
        return 'Success!'
    except GithubException as e:
        raise HTTPException(status_code=e._GithubException__status, detail=e._GithubException__data)
    except AssertionError as e:
        raise HTTPException(status_code=400, detail="title is None")

