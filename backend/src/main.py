import os
import secrets
import sqlite3
from enum import Enum
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path

from . import database

db_path = Path(os.environ["DATABASE_PATH"])
schema_path = Path(os.environ["SCHEMA_PATH"])

root = Path(os.environ["PROJECT_ROOT"])
templates = Jinja2Templates(directory=root / "frontend" / "templates")

class EntityType(str, Enum):
    olympiads = "olympiads"
    players   = "players"
    teams     = "teams"
    events    = "events"

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
    if response: return response

    cursor = conn.execute("SELECT id, name, version FROM olympiads")
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["olympiads"]
    response = templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )

    return response

@app.get("/api/edit/{entities}/{item_id}/{version}")
async def get_edit_textbox(request: Request, entities: EntityType, item_id: int, version: int,conn = Depends(get_db)):
    row = conn.execute(f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (item_id,)).fetchone()
    if not row:
        return templates.TemplateResponse(request, "entity_not_found.html", {"entities": entities})
    if row["version"] != version:
        return templates.TemplateResponse(request, "entity_version_conflict.html", {"entities": entities})

    return templates.TemplateResponse(
        request, "edit_entity.html",
        {
            "curr_name": row["name"],
            "entities": entities.value,
            "id": row["id"],
            "version": row["version"]
        }
    )

@app.get("/api/{entities}")
async def list_entities(request: Request, entities: EntityType, conn = Depends(get_db)):
    session_id = request.state.session_id

    items = conn.execute(
        f"""
        SELECT e.id, e.name, e.version
        FROM {entities.value} e
        JOIN olympiads o ON o.id = e.olympiad_id
        JOIN sessions s ON s.selected_olympiad_id = o.id AND s.selected_olympiad_version = o.version AND s.id = ?
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


@app.get("/api/olympiads/{id}/{version}")
async def select_olympiad(request: Request, id: int, version: int, conn = Depends(get_db)):
    """Select an olympiad and update the olympiad badge"""
    olympiad = conn.execute("SELECT id, name, version FROM olympiads WHERE id = ?", (id,)).fetchone()

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
async def create_olympiad(request: Request, name: str = Form(...), pin: str = Form(None), conn = Depends(get_db)):
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
        response.headers["HX-Retarget"] = "#modal-container"
        return response
    
    try:
        row = conn.execute(
            f"INSERT INTO olympiads (name, pin) VALUES (?, ?) RETURNING id",
            (name, pin)
        ).fetchone()
        olympiad_id = row[0]
    except sqlite3.IntegrityError:
        response = templates.TemplateResponse(
            request, "olympiad_name_duplicate.html",
        )
        return response
    
    # Happy path - return updated list                                                                                                                                                                                                               

    conn.execute(
        f"INSERT INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
        (request.state.session_id, olympiad_id)
    )
    conn.commit()

    cursor = conn.execute("SELECT id, name, version FROM olympiads")                                                                                                                                                                                 
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]                                                                                                                                          
    placeholder = entity_list_form_placeholder["olympiads"]                                                                                                                                                                                          
    response = templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )
    return response

@app.put("/api/{entities}/{item_id}/{version}")
async def rename_entity(request: Request, entities: EntityType, item_id: int, version: int, name: str = Form(...),conn = Depends(get_db)):
    session_id = request.state.session_id

    # Check if entity exists and version matches
    row = conn.execute(f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (item_id,)).fetchone()

    if not row:
        response = templates.TemplateResponse(request, "entity_not_found.html", {"entities": entities.value})
        response.headers["HX-Retarget"] = "#main-content"

    if row["version"] != version:
        # Version mismatch - entity was modified
        response = templates.TemplateResponse(request, "entity_version_conflict.html", {"entities": entities.value})
        response.headers["HX-Retarget"] = "#main-content"

    # TODO: let's consider just the case of renaming an olympiad for now, then you must handle renaming other entities
    row = conn.execute(
        "SELECT olympiad_id FROM session_olympiad_auth WHERE session_id = ? AND olympiad_id = ?", (session_id, item_id)
    ).fetchone()
    if not row:
        # the action that I want to do is to validate the pin the user will pass and upon successfull validation call again
        # rename_entity, the successfull validation has stored in session_olympiad_auth info about this olympiad, so this
        # time the check will pass, to do it I must pass to pin modal not only validate_pin as action
        # but also what the code should call upon successfull validation
        response = templates.TemplateResponse(
            request, "pin_modal.html", {"name": name, "action": f"/api/validate_pin", "submit_text": "Rinomina"}
        )
        response.headers["HX-Retarget"] = "#modal-container"

    try:
        conn.execute(
            f"UPDATE {entities.value} SET name = ?, version = version + 1 WHERE id = ? AND version = ?",
            (name, item_id, version)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return templates.TemplateResponse(
            request, "duplicate_name_error.html",
            {"message": f"Un elemento con questo nome esiste già", "entities": entities.value}
        )

    # Return the updated entity element
    updated = conn.execute(f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (item_id,)).fetchone()

    # Re-render just the single entity element
    hx_target = "#olympiad-badge-container" if entities == "olympiads" else "#main-content"
    return templates.TemplateResponse(
        request, "entity_element.html",
        {
            "item": {"id": updated["id"], "name": updated["name"], "version": updated["version"]},
            "entities": entities,
            "hx_target": hx_target
        }
    )

@app.post("/api/validate_pin")
async def validate_pin(
    request: Request,
    pin: str = Form(...),
    olympiad_id: str = Form(...),
    conn = Depends(get_db)
):
    # TODO: validate_pin (and where it's called) must be modified to pass explicitly the olympiad_id paramter
    #       I do not want to always use the selected_olympaid_id by the session
    #       for example: I want to be able to rename an olympiad even if it is not the currently selected
    session_id = request.state.session_id

    # Get the selected olympiad and its PIN
    row = conn.execute("SELECT o.id, o.pin FROM olympiads o WHERE o.id = ?", (olympiad_id)).fetchone()

    if not row:
        # Olympiad was deleted

        # What I really do not like here is that I must remember to close the modal
        # What I would really prefer is the modal to close automatically when the request that start with
        # pin modal is done
        response = templates.TemplateResponse(request, "olympiad_not_found.html")
        response.headers["HX-Retarget"] = "#main-content"
        response.headers["HX-Trigger-After-Settle"] = "closeModal"
        return response

    if row["pin"] != pin:
        # Wrong PIN - show error in modal
        response = templates.TemplateResponse(
            request, "pin_modal.html",
            {"action": "/api/validate_pin", "error": "PIN errato"}
        )
        response.headers["HX-Retarget"] = "#modal-container"
        return response

    # PIN is correct - grant access
    conn.execute(
        "INSERT OR IGNORE INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
        (session_id, row["id"])
    )
    conn.commit()

    # Here we should call the the function create_player with the player_name
    # and then we should rerender the players list
    # what I do not like is that in this way I have to make all the checks again

    response = HTMLResponse("")
    response.headers["HX-Trigger-After-Settle"] = "closeModal"
    response.headers["HX-Trigger"] = "retryPendingAction"
    return response


@app.post("/api/players")
async def create_player(
    request: Request, name: str = Form(...), conn = Depends(get_db)
):
    # Check that the olympiad still exist
    session_id = request.state.session_id
    session = conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()
    response = check_selected_olympiad(conn, request, session)
    if response:
        return response

    row = conn.execute(
        """
        SELECT s.selected_olympiad_id
        FROM sessions s
        JOIN session_olympiad_auth soa ON soa.olympiad_id = s.selected_olympiad_id AND soa.session_id = s.id
        WHERE s.id = ?
        """,
        (session_id,)
    ).fetchone()
    if not row:
        # Show the pin modal that must have validate pin as action

        # TODO: if I am starting the trip to the pin_modal.html how can I complete
        #       the action that I was doing upon successfull validation?
        #       I must pass to the pin modal the api call (and parameters needed)
        #       upon successfully calll to validate_pin
        #       why I did not need it for create_olympiads? because create_olympiads pass to the pin modal itself

        response = templates.TemplateResponse(
            request, "pin_modal.html", {"action": "/api/validate_pin"}
        )
        response.headers["HX-Retarget"] = "#modal-container"
        return response

    # I can add the player
    try:
        conn.execute(
            f"INSERT INTO players (olympiad_id, name) VALUES (?, ?)",
            (row["selected_olympiad_id"], name)
        )
    except sqlite3.IntegrityError:
        return templates.TemplateResponse(
            request, "duplicate_name_error.html",
            {"message": "Un giocatore con questo nome è già presente", "entities": "players"}
        )
    conn.commit()

    cursor = conn.execute(
        f"SELECT p.id, p.name, p.version FROM players p JOIN olympiads o ON o.id = p.olympiad_id WHERE o.id = ?",
        (row["selected_olympiad_id"],)
    )
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["players"]

    return templates.TemplateResponse(
        request, "entity_list.html", {"entities": "players", "placeholder": placeholder, "items": rows}
    )


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000)
