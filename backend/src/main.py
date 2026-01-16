import os
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path
from contextlib import asynccontextmanager

from . import database

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = Path(os.environ["DATABASE_PATH"])
    schema_path = Path(os.environ["SCHEMA_PATH"])
    database.init_db(db_path, schema_path)

    yield
    # TODO: for now I always want to delete the db when the application shutdown
    db_path.unlink()

app = FastAPI(lifespan=lifespan)

root = Path(os.environ["PROJECT_ROOT"])
templates = Jinja2Templates(directory=root / "frontend" / "templates")

@app.get("/health")
async def get_health():
    return JSONResponse(200)

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML file"""
    html_path = root / "frontend" / "index.html"
    return HTMLResponse(content=html_path.read_text())

@app.get("/{filename}.css")
async def serve_css(filename: str):
    """Serve CSS files from frontend directory"""
    css_path = root / "frontend" / f"{filename}.css"
    return Response(content=css_path.read_text(), media_type="text/css")

@app.get("/api/{entities}")
async def get_entities(request: Request, entities: str):
    # TODO: let's retrieve the data from a database
    data = {
        "olympiads": ("olimpiadi", ["OlympiadA", "OlympiadB", "OlympiadC", "OlympiadD", "OlympiadE"]),
        "players": ("giocatori", ["PlayerA", "PlayerB", "PlayerC", "PlayerD", "PlayerE"]),
        "events": ("eventi", ["EventA", "EventB", "EventC", "EventD", "EventE"]),
    }
    return templates.TemplateResponse(
        request,
        "entity_list.html",
        {"entity_type": data[entities][0], "items": data[entities][1]}
    )

if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000)

