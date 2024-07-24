from fastapi import FastAPI, Request
from clip_client import Client
import database
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(directory="templates")

@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse(request=request, name="index.jinja")

class SearchLanguage(str):
    en = "en"
    jp = "jp"

@app.get("/search")
async def search(query: str, lang: SearchLanguage = SearchLanguage.en, cache: bool = True):
    cursors = database.get_cursor()


