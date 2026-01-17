import os
import uvicorn
from fastapi import FastAPI, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path
from contextlib import asynccontextmanager

from . import database

db_path = Path(os.environ["DATABASE_PATH"])
schema_path = Path(os.environ["SCHEMA_PATH"])

root = Path(os.environ["PROJECT_ROOT"])
templates = Jinja2Templates(directory=root / "frontend" / "templates")

entity_labels = { "olympiads": "olimpiadi", "players": "giocatori", "events": "eventi" }

@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db(db_path, schema_path)
    database.seed_dummy_data(db_path)

    yield
    # TODO: for now I always want to delete the db when the application shutdown
    db_path.unlink()

app = FastAPI(lifespan=lifespan)

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

@app.post("/api/{entities}")
async def create_entity(request: Request, entities: str, name: str = Form(...), olympiad_id: str = Form(default="")):
    conn = database.get_connection(db_path)
    try:
        if entities == "olympiads":
            try:
                # query that add an olympiad with name
                cursor = conn.execute(f"INSERT INTO olympiads (name, pin) VALUES ({name}, {9999})")
            except:
                # handle case in which name already exist
                # I want to return the html of a modal window with a message that says "olympiad name already used"
                # and a button with "refresh page" that when is pressed will refresh the olympiads page
                # which means executing the query that retrieve the list of (olympiad_id, name, version) and render the entity_list.html template
                # which basically means reloading the olympiads page almost from scratch except for the olympiad badge
        else:
            raise NotImplementedError
    finally:
        conn.close()

@app.get("/api/{entities}")
async def get_entities(request: Request, entities: str, olympiad_id: str = ""):
    conn = database.get_connection(db_path)
    try:
        if entities == "olympiads":
            cursor = conn.execute(f"SELECT id, name, version FROM olympiads")
            rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
        else:
            if olympiad_id:
                cursor = conn.execute(f"SELECT id FROM olympiads WHERE id = {olympiad_id}")
                row = cursor.fetchone()
                if row:
                    cursor = conn.execute(
                        f"SELECT e.id, e.name, e.version FROM {entities} e JOIN olympiads o ON o.id = e.olympiad_id WHERE o.id = {olympiad_id}"
                    )
                    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
                else:
                    return templates.TemplateResponse(request, "olympiad_not_found.html")
            else:
                return templates.TemplateResponse(request, "select_olympiad_required.html", {"entity_label": entity_labels[entities]})
    finally:
        conn.close()

    return templates.TemplateResponse(
        request,
        "entity_list.html",
        {"entities": entities, "entity_label": entity_labels[entities], "items": rows}
    )

@app.get("/api/olympiads/{olympiad_id}")
async def select_olympiad(request: Request, olympiad_id: int):
    """Select an olympiad and update the olympiad badge"""
    db_path = Path(os.environ["DATABASE_PATH"])
    olympiad = database.get_olympiad(db_path, olympiad_id)
    if olympiad is None:
        return HTMLResponse(content="Olympiad not found", status_code=404)
    return templates.TemplateResponse(request, "olympiad_badge.html", {"olympiad": olympiad})


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000)

