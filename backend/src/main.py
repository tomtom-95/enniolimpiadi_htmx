import html
import json
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


def get_badge_oob(session_id: str, conn) -> str:
    """
    Returns OOB HTML to update the olympiad badge based on current session state.
    Call this after any operation that might affect the selected olympiad.

    Returns the appropriate badge content:
    - The olympiad name if one is selected and exists
    - "Olympiad badge" if none selected or the selected one was deleted
    """
    row = conn.execute(
        f"SELECT o.name FROM sessions s LEFT JOIN olympiads o ON s.selected_olympiad_id = o.id WHERE s.id = ?",
        (session_id,)
    ).fetchone()

    name = row["name"] if row and row["name"] else "Olympiad badge"
    return f'<div id="olympiad-badge-container" hx-swap-oob="innerHTML">{html.escape(name)}</div>'

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
    conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()

    cursor = conn.execute("SELECT id, name, version FROM olympiads")
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["olympiads"]
    response = templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )

    return response

@app.get("/api/edit/{entities}/{item_id}/{version}")
async def get_edit_textbox(
    request: Request, entities: EntityType, item_id: int, version: int, conn = Depends(get_db)
):
    session_id = request.state.session_id

    row = conn.execute(f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (item_id,)).fetchone()

    if not row:
        if entities == EntityType.olympiads:
            session_row = conn.execute("SELECT selected_olympiad_id FROM sessions WHERE id = ?", (session_id,)).fetchone()
            if session_row["selected_olympiad_id"] == item_id:
                conn.execute(
                    "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                    (session_id,)
                )
                conn.commit()

        entity_html = templates.get_template("entity_deleted_oob.html").render()
        badge_oob = get_badge_oob(session_id, conn)
        return HTMLResponse(entity_html + badge_oob)

    if row["version"] != version:
        if entities == EntityType.olympiads:
            session_row = conn.execute(
                "SELECT selected_olympiad_id FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if session_row and session_row["selected_olympiad_id"] == item_id:
                conn.execute(
                    "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?", (row["version"], session_id)
                )
                conn.commit()

        item = {"id": row["id"], "name": row["name"], "version": row["version"]},
        entity_html = templates.get_template("entity_rename_oob.html").render(item=item, entities=entities.value)
        badge_oob = get_badge_oob(session_id, conn)
        return HTMLResponse(entity_html + badge_oob)

    response = templates.TemplateResponse(
        request, "edit_entity.html",
        {
            "curr_name": row["name"],
            "entities": entities.value,
            "id": row["id"],
            "version": row["version"]
        }
    )

    return response


@app.get("/api/{entities}/{item_id}/{version}/cancel-edit")
async def cancel_edit(
    request: Request, entities: EntityType, item_id: int, version: int, conn = Depends(get_db)
):
    """Cancel editing and return the entity element in its current state."""
    session_id = request.state.session_id

    row = conn.execute(
        f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (item_id,)
    ).fetchone()

    session_row = conn.execute(
        "SELECT selected_olympiad_id FROM sessions WHERE id = ?", (session_id,)
    ).fetchone()

    badge_oob = ""
    if not row:
        # Entity was deleted while editing
        if entities == EntityType.olympiads and session_row["selected_olympiad_id"] == item_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                (session_id,)
            )
            conn.commit()

            badge_oob = get_badge_oob(session_id, conn)

        entity_html = templates.get_template("entity_deleted_oob.html").render()
        return HTMLResponse(entity_html + badge_oob)

    item = {"id": row["id"], "name": row["name"], "version": row["version"]}

    if row["version"] != version:
        # Entity was renamed while editing - update session if needed
        if entities == EntityType.olympiads and session_row["selected_olympiad_id"] == item_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
                (row["version"], session_id)
            )
            conn.commit()

            badge_oob = get_badge_oob(session_id, conn)
            entity_html = templates.get_template("entity_rename_oob.html").render(item=item, entities=entities.value)
            return HTMLResponse(entity_html + badge_oob)

    hx_target = "#olympiad-badge-container" if entities.value == "olympiads" else "#main-content"

    entity_html = templates.get_template("entity_element.html").render(
        item=item, entities=entities.value, hx_target=hx_target
    )

    return HTMLResponse(entity_html + badge_oob)


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
        placeholder = entity_list_form_placeholder[entities.value]
        return templates.TemplateResponse(
            request, "entity_list.html",
            {"entities": entities.value, "placeholder": placeholder, "items": items}
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
            request, "select_olympiad_required.html", {"message": select_olympiad_message[entities.value]}
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
    placeholder = entity_list_form_placeholder[entities.value]
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
        # response.headers["HX-Reswap"] = "innerHTML"
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
        # response.headers["HX-Reswap"] = "innerHTML"
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

@app.put("/api/{entities}/{id}/{version}")
async def rename_entity(
    request: Request, entities: EntityType, id: int, version: int, name: str = Form(...), conn = Depends(get_db)
):
    session_id = request.state.session_id

    # Check if entity exists and version matches
    row = conn.execute(f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (id,)).fetchone()

    if not row:
        response = templates.TemplateResponse(request, "entity_not_found.html", {"entities": entities.value})
        response.headers["HX-Retarget"] = "#main-content"
        return response

    if row["version"] != version:
        # Version mismatch - entity was modified
        response = templates.TemplateResponse(request, "entity_version_conflict.html", {"entities": entities.value})
        response.headers["HX-Retarget"] = "#main-content"
        return response

    # TODO: let's consider just the case of renaming an olympiad for now, then you must handle renaming other entities
    row = conn.execute(
        "SELECT olympiad_id FROM session_olympiad_auth WHERE session_id = ? AND olympiad_id = ?", (session_id, id)
    ).fetchone()
    if not row:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/validate_pin",
                "olympiad_id": id,
                "callback_params": json.dumps({
                    "entities": entities.value,
                    "item_id": id,
                    "version": version,
                    "name": name
                })
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    try:
        conn.execute(
            f"UPDATE {entities.value} SET name = ?, version = version + 1 WHERE id = ? AND version = ?",
            (name, id, version)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return templates.TemplateResponse(
            request, "duplicate_name_error.html",
            {"message": f"Un elemento con questo nome esiste già", "entities": entities.value}
        )

    # Return the updated entity element
    updated = conn.execute(f"SELECT id, name, version FROM {entities.value} WHERE id = ?", (id,)).fetchone()

    # Re-render just the single entity element
    hx_target = "#olympiad-badge-container" if entities.value == "olympiads" else "#main-content"

    item = {"id": updated["id"], "name": updated["name"], "version": updated["version"]}
    entity_html = templates.get_template(
        "entity_element.html"
    ).render(item=item, entities=entities.value, hx_target=hx_target)

    # Always append badge OOB - the function determines the correct content
    badge_oob = get_badge_oob(session_id, conn)

    return HTMLResponse(entity_html + badge_oob)

def rename_entity_after_pin(request, conn, params):
    session_id = request.state.session_id

    entities = EntityType(params["entities"])
    item_id = params["item_id"]
    version = params["version"]
    name = params["name"]

    try:
        updated = conn.execute(
            f"UPDATE {entities.value} SET name = ?, version = version + 1 WHERE id = ? AND version = ? RETURNING id, name, version",
            (name, item_id, version)
        ).fetchone()
        conn.commit()
    except sqlite3.IntegrityError:
        return templates.TemplateResponse(
            request, "duplicate_name_error.html",
            {"message": f"Un elemento con questo nome esiste già", "entities": entities.value}
        )
    
    hx_target = "#olympiad-badge-container" if entities.value == "olympiads" else "#main-content"

    item = {"id": updated["id"], "name": updated["name"], "version": updated["version"]}
    entity_html = templates.get_template("entity_element.html").render(
        item=item, entities=entities.value, hx_target=hx_target
    )

    badge_oob = get_badge_oob(session_id, conn)

    response = HTMLResponse(entity_html + badge_oob)
    response.headers["HX-Retarget"] = f"#{entities.value}-{item_id}"
    response.headers["HX-Reswap"] = "outerHTML"

    return response

@app.post("/api/validate_pin")
async def validate_pin(
    request: Request,
    pin: str = Form(...),
    olympiad_id: int = Form(...),
    callback_params: str = Form(...),
    conn = Depends(get_db)
):
    session_id = request.state.session_id

    # Get the olympiad and its PIN
    row = conn.execute("SELECT id, pin FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    if not row:
        response = templates.TemplateResponse(request, "olympiad_not_found.html")
        response.headers["HX-Retarget"] = "#main-content"
        response.headers["HX-Trigger-After-Settle"] = "closeModal"
        return response

    if row["pin"] != pin:
        # Wrong PIN - show error in modal
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/validate_pin",
                "olympiad_id": olympiad_id,
                "callback_params": callback_params,
                "error": "PIN errato"
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    # PIN is correct - grant access
    conn.execute(
        "INSERT OR IGNORE INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
        (session_id, row["id"])
    )
    conn.commit()

    params = json.loads(callback_params)
    return rename_entity_after_pin(request, conn, params)


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
        #       upon successfully calll to validate_pin
        #       why I did not need it for create_olympiads? because create_olympiads pass to the pin modal itself

        response = templates.TemplateResponse(
            request, "pin_modal.html", {"action": "/api/validate_pin"}
        )
        response.headers["HX-Retarget"] = "#modal-container"
        # response.headers["HX-Reswap"] = "innerHTML"
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
