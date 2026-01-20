import os
import sqlite3
import secrets
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

def check_selected_olympiad(conn, request, session):
    """
    Check if the selected olympiad still exists and has the correct version.
    Updates session state and returns an appropriate response if needed.

    Returns:
        None if no selected olympiad or everything is up to date.
        A Response object if the olympiad was deleted or its version changed.
    """

    selected_olympiad_id = session["selected_olympiad_id"]
    selected_olympiad_version = session["selected_olympiad_version"]
    if not selected_olympiad_id:
        return None

    olympiad = conn.execute(
        "SELECT id, name, version FROM olympiads WHERE id = ?",
        (selected_olympiad_id,)
    ).fetchone()

    if not olympiad:
        conn.execute(
            "UPDATE sessions SET (selected_olympiad_id, selected_olympiad_version) = (NULL, NULL) WHERE id = ?",
            (request.state.session_id,)
        )
        conn.commit()
        return templates.TemplateResponse(request, "olympiad_not_found.html")

    if olympiad["version"] != selected_olympiad_version:
        conn.execute(
            "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
            (olympiad["version"], request.state.session_id)
        )
        conn.commit()
        return templates.TemplateResponse(
            request, "olympiad_name_changed.html",
            {"olympiad_name": olympiad["name"]}
        )

    return None

@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db(db_path, schema_path)
    database.seed_dummy_data(db_path)

    yield
    # TODO: for now I always want to delete the db when the application shutdown
    db_path.unlink()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def session_middleware(request: Request, call_next):
    session_id = request.cookies.get("session")
    conn = database.get_connection(db_path)
    try:
        if session_id:
            cursor = conn.execute("SELECT id FROM sessions WHERE id = ?", (session_id,))
            if not cursor.fetchone():
                session_id = None

        if not session_id:
            session_id = secrets.token_urlsafe(32)
            conn.execute("INSERT INTO sessions (id) VALUES (?)", (session_id,))
            conn.commit()

        request.state.session_id = session_id

        response = await call_next(request)
        response.set_cookie("session", session_id, httponly=True, max_age=86400)
    finally:
        conn.close()

    return response

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

@app.get("/api/olympiads")
async def list_olympiads(request: Request, conn = Depends(get_db)):
    session_id = request.state.session_id
    session = conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()

    response = check_selected_olympiad(conn, request, session)
    if response:
        return response

    cursor = conn.execute("SELECT id, name, version FROM olympiads")
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["olympiads"]
    response = templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )

    return response

ALLOWED_ENTITIES = {"players", "events", "teams"}

@app.get("/api/{entities}")
async def list_entities(request: Request, entities: str, conn = Depends(get_db)):
    if entities not in ALLOWED_ENTITIES:
        return JSONResponse(status_code=404, content={"detail": "Not found"})

    session_id = request.state.session_id

    # Happy path: lean query that only returns entities if olympiad is valid and version matches.
    # Uses INNER JOINs so rows only returned when everything is valid.
    items = conn.execute(
        f"""
        SELECT e.id, e.name, e.version
        FROM {entities} e
        JOIN olympiads o ON o.id = e.olympiad_id
        JOIN sessions s ON s.selected_olympiad_id = o.id
                       AND s.selected_olympiad_version = o.version
                       AND s.id = ?
        """,
        (session_id,)
    ).fetchall()

    if items:
        placeholder = entity_list_form_placeholder[entities]
        return templates.TemplateResponse(
            request, "entity_list.html",
            {"entities": entities, "placeholder": placeholder, "items": items}
        )

    # No rows returned - diagnose why with a single query
    diag = conn.execute(
        """
        SELECT
            s.selected_olympiad_id,
            s.selected_olympiad_version as session_version,
            o.id as olympiad_id,
            o.name as olympiad_name,
            o.version as olympiad_version
        FROM sessions s
        LEFT JOIN olympiads o ON o.id = s.selected_olympiad_id
        WHERE s.id = ?
        """,
        (session_id,)
    ).fetchone()

    # Case 1: No olympiad selected
    if diag["selected_olympiad_id"] is None:
        return templates.TemplateResponse(
            request, "select_olympiad_required.html", {"message": select_olympiad_message[entities]}
        )

    # Case 2: Olympiad was deleted
    if diag["olympiad_id"] is None:
        conn.execute(
            "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
            (session_id,)
        )
        conn.commit()
        return templates.TemplateResponse(request, "olympiad_not_found.html")

    # Case 3: Olympiad version mismatch (was renamed)
    if diag["olympiad_version"] != diag["session_version"]:
        conn.execute(
            "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
            (diag["olympiad_version"], session_id)
        )
        conn.commit()
        return templates.TemplateResponse(
            request, "olympiad_name_changed.html",
            {"olympiad_name": diag["olympiad_name"]}
        )

    # Case 4: Valid olympiad, just no entities yet
    placeholder = entity_list_form_placeholder[entities]
    return templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": entities, "placeholder": placeholder, "items": []}
    )


@app.get("/api/olympiads/{olympiad_id}")
async def select_olympiad(
    request: Request,
    olympiad_id: int,
    version: int,
    conn = Depends(get_db)
):
    """Select an olympiad and update the olympiad badge"""

    session_id = request.state.session_id
    session = conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()

    # Check if currently selected olympiad is still valid (sync session state)
    response = check_selected_olympiad(conn, request, session)
    if response:
        response.headers["HX-Retarget"] = "#main-content"
        return response

    # Validate the newly selected olympiad exists and version matches
    olympiad = conn.execute(
        "SELECT id, name, version FROM olympiads WHERE id = ?",
        (olympiad_id,)
    ).fetchone()

    if not olympiad:
        response = templates.TemplateResponse(request, "olympiad_not_found.html")
        response.headers["HX-Retarget"] = "#main-content"
        return response

    if olympiad["version"] != version:
        response = templates.TemplateResponse(
            request, "olympiad_name_changed.html", {"olympiad_name": olympiad["name"]}
        )
        response.headers["HX-Retarget"] = "#main-content"
        return response

    # Update session with the newly selected olympiad
    conn.execute(
        "UPDATE sessions SET (selected_olympiad_id, selected_olympiad_version) = (?, ?) WHERE id = ?",
        (olympiad["id"], olympiad["version"], request.state.session_id)
    )
    conn.commit()

    return templates.TemplateResponse(
        request, "olympiad_badge.html",
        {"olympiad": {"id": olympiad["id"], "name": olympiad["name"], "version": olympiad["version"]}}
    )

@app.post("/api/olympiads")
async def create_olympiad(
    request: Request,
    name: str = Form(...),
    pin: str = Form(None),
    conn = Depends(get_db)
):
    # First call (no PIN): show PIN modal
    if pin is None:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "name": name,
                "action": "/api/olympiads",
                "submit_text": "Crea"
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        return response

    # PIN provided but invalid
    if len(pin) != 4:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "name": name,
                "action": "/api/olympiads",
                "submit_text": "Crea",
                "error": "Il PIN deve essere composto da 4 cifre"
            }
        )
        return response

    # TODO: insert olympiad into database
    #       try to insert the olympiad in the database
    #       how can it fail? olympiad name already exist, must show a message, I can closeModal,
    #       retarget main-content and have a message there similar to the one in olympiad_not_found.html
    #       store in auth table data for this session user, add a row with current session_id and olympiad_id to session_olympiad_auth
    response = HTMLResponse("")
    response.headers["HX-Trigger-After-Settle"] = "closeModal"
    return response

@app.post("/api/validate_pin")
async def validate_pin(
    request: Request,
    olympiad_id: str = Form(...),
    pin: str = Form(...),
    conn = Depends(get_db)
):
    cursor = conn.execute("SELECT pin FROM olympiads WHERE id = ?", (olympiad_id,))
    row = cursor.fetchone()

    if not row or row["pin"] != pin:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "olympiad_id": olympiad_id,
                "action": "/api/validate_pin",
                "error": "PIN errato",
                "pin": ""
            }
        )
        return response

    # Success - set cookie and trigger retry
    response = HTMLResponse("")
    response.set_cookie(
        key=f"olympiad_auth_{olympiad_id}",
        value=f"valid_{olympiad_id}",  # TODO: sign this properly with itsdangerous
        httponly=True,
        max_age=86400  # 24 hours
    )
    response.headers["HX-Trigger-After-Settle"] = "closeModal"
    response.headers["HX-Trigger"] = "retryPendingAction"
    return response


@app.post("/api/players")
async def create_player(
    request: Request, name: str = Form(...), olympiad_id: str = Form(...), conn = Depends(get_db)
):
    cookie = request.cookies.get(f"olympiad_auth_{olympiad_id}")

    # TODO: must check that
    if not cookie:
        # Return PIN modal, but DON'T replace the form - target modal container instead
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "olympiad_id": olympiad_id,
                "action": "/api/validate_pin"
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

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


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000)
