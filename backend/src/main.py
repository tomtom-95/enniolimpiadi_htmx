import os
import random
import sqlite3
import uvicorn
from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path
from contextlib import asynccontextmanager

from . import database

db_path = Path(os.environ["DATABASE_PATH"])
schema_path = Path(os.environ["SCHEMA_PATH"])

root = Path(os.environ["PROJECT_ROOT"])
templates = Jinja2Templates(directory=root / "frontend" / "templates")

entity_list_form_placeholder = {
    "olympiads": "Aggiungi una nuova olimpiade",
    "players": "Aggiungi un nuovo giocatore",
    "events": "Aggiungi un nuovo evento",
    "teams": "Aggiungi un nuovo team",
}

select_olympiad_message = {
    "players": "Seleziona un'olimpiade per visualizzare tutti i giocatori",
    "events": "Seleziona un'olimpiade per visualizzare tutti gli eventi",
    "teams": "Seleziona un'olimpiade per visualizzare tutti i team",
}

def get_db():
    conn = database.get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()

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

@app.post("/api/olympiads")
async def create_olympiad(request: Request, name: str = Form(...)):
    response = templates.TemplateResponse(
        request, "create_olympiad_pin.html", {"name": name}
    )
    response.headers["HX-Retarget"] = "#modal-container"
    return response

@app.post("/api/olympiads/confirm_creation")
async def create_olympiad_with_pin(
    request: Request, name: str = Form(...), pin: str = Form(...), conn = Depends(get_db)
):
    if len(pin) != 4:
        response = templates.TemplateResponse(
            request, "create_olympiad_pin.html",
            {"name": name, "error": "Il PIN deve essere composto da 4 cifre"}
        )
        response.headers["HX-Retarget"] = "#modal-container"
        return response
    try:
        conn.execute("INSERT INTO olympiads (name, pin) VALUES (?, ?)", (name, pin))
    except sqlite3.IntegrityError:
        return templates.TemplateResponse(
            request, "duplicate_name_error.html",
            {"message": "Una olimpiade con questo nome è già presente", "entities": "olympiads"}
        )
    conn.commit()
    cursor = conn.execute("SELECT id, name, version FROM olympiads")
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["olympiads"]
    return templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )

@app.post("/api/players")
async def create_player(request: Request, name: str = Form(...), olympiad_id: str = Form(...), conn = Depends(get_db)):
    cursor = conn.execute(f"SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id))
    row = cursor.fetchone()
    if not row:
        return templates.TemplateResponse(request, "olympiad_not_found.html")

    try:
        conn.execute(f"INSERT INTO players (olympiad_id, name) VALUES (?, ?)", (olympiad_id, name))
    except sqlite3.IntegrityError:
        return templates.TemplateResponse(
            request, "duplicate_name_error.html", {"message": "Un giocatore con questo nome è già presente", "entities": "players"}
        )
    conn.commit()
    cursor = conn.execute(
        f"SELECT p.id, p.name, p.version FROM players p JOIN olympiads o ON o.id = p.olympiad_id WHERE o.id = ?",
        (olympiad_id,)
    )
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["players"]
    return templates.TemplateResponse(
        request, "entity_list.html", {"entities": "players", "placeholder": placeholder, "items": rows}
    )

@app.get("/api/olympiads")
async def get_olympiads(request: Request, conn = Depends(get_db)):
    cursor = conn.execute(f"SELECT id, name, version FROM olympiads")
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["players"]
    return templates.TemplateResponse(
        request, "entity_list.html", {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )

# TODO: fix security issues
@app.get("/api/{entities}")
async def get_entities(request: Request, entities: str, olympiad_id: str = "", conn = Depends(get_db)):
    if olympiad_id:
        cursor = conn.execute(f"SELECT id FROM olympiads WHERE id = {olympiad_id}")
        row = cursor.fetchone()
        if row:
            cursor = conn.execute(
                f"SELECT e.id, e.name, e.version FROM {entities} e JOIN olympiads o ON o.id = e.olympiad_id WHERE o.id = ?",
                (olympiad_id,)
            )
            rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
            placeholder = entity_list_form_placeholder[entities]
            return templates.TemplateResponse(
                request, "entity_list.html", {"entities": entities, "placeholder": placeholder, "items": rows}
            )
        else:
            return templates.TemplateResponse(request, "olympiad_not_found.html")
    else:
        return templates.TemplateResponse(request, "select_olympiad_required.html", {"message": select_olympiad_message[entities]})

@app.get("/api/olympiads/{olympiad_id}")
async def select_olympiad(request: Request, olympiad_id: int, conn = Depends(get_db)):
    """Select an olympiad and update the olympiad badge"""

    cursor = conn.execute("SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id,))
    row = cursor.fetchone()
    if not row:
        response = templates.TemplateResponse(request, "olympiad_not_found.html")
        response.headers["HX-Retarget"] = "#main-content"
        return response

    olympiad = {"id": row["id"], "name": row["name"], "version": row["version"]}
    return templates.TemplateResponse(request, "olympiad_badge.html", {"olympiad": olympiad})


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000)

