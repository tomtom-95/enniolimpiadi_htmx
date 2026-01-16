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
    database.seed_dummy_data(db_path)

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
    entity_labels = {
        "olympiads": "olimpiadi",
        "players": "giocatori",
        "events": "eventi",
    }
    db_path = Path(os.environ["DATABASE_PATH"])
    items = database.get_entities(db_path, entities)
    return templates.TemplateResponse(
        request,
        "entity_list.html",
        {"entities": entities, "entity_label": entity_labels[entities], "items": items}
    )

@app.get("/api/olympiads/{olympiad_id}")
async def select_olympiad(request: Request, olympiad_id: int):
    """Select an olympiad and update the olympiad badge"""
    db_path = Path(os.environ["DATABASE_PATH"])
    olympiad = database.get_olympiad(db_path, olympiad_id)
    if olympiad is None:
        return HTMLResponse(content="Olympiad not found", status_code=404)
    return templates.TemplateResponse(
        request,
        "olympiad_badge.html",
        {"olympiad": olympiad}
    )


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000)

