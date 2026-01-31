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


class AuthResult(Enum):
    AUTHENTICATED = "authenticated"
    NOT_AUTHENTICATED = "not_authenticated"
    OLYMPIAD_DELETED = "olympiad_deleted"


# Registry for PIN-protected actions
PIN_PROTECTED_ACTIONS = {}

def pin_protected(action_name):
    """Decorator to register a function as a PIN-protected action"""
    def decorator(func):
        PIN_PROTECTED_ACTIONS[action_name] = func
        return func
    return decorator


BADGE_CHANGED_TRIGGER = "badgeChanged"


def trigger_badge_update(response: Response) -> None:
    """Add HX-Trigger header to make the badge refresh itself."""
    response.headers["HX-Trigger"] = BADGE_CHANGED_TRIGGER


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


@app.get("/api/badge")
async def get_badge(request: Request, conn = Depends(get_db)):
    """Return the current olympiad badge content based on session state."""
    session_id = request.state.session_id
    row = conn.execute(
        "SELECT o.name FROM sessions s LEFT JOIN olympiads o ON s.selected_olympiad_id = o.id WHERE s.id = ?",
        (session_id,)
    ).fetchone()

    if row and row["name"]:
        name = html.escape(row["name"])
    else:
        name = "Olympiad badge"

    return HTMLResponse(name)

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

@app.get("/api/{entities}/{item_id}/{version}/edit")
async def get_edit_textbox(
    request: Request, entities: EntityType, item_id: int, version: int, name: str
):

    response = templates.TemplateResponse(
        request, "edit_entity.html",
        {
            "curr_name": name,
            "entities": entities.value,
            "id": item_id,
            "version": version
        }
    )
    return response


@app.get("/api/{entities}/{item_id}/{version}/cancel-edit")
async def cancel_edit(
    request: Request, entities: EntityType, item_id: int, version: int, name: str,
):
    hx_target = "#olympiad-badge-container" if entities.value == "olympiads" else "#main-content"
    item = {"id": item_id, "name": name, "version": version}
    response = templates.TemplateResponse(
        request,
        "entity_element.html",
        {
            "item": item,
            "entities": entities.value,
            "hx_target": hx_target
        }
    )
    return response

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
async def create_olympiad(
    request: Request, name: str = Form(None), pin: str = Form(None), params: str = Form(None), conn = Depends(get_db)
):
    # If params provided (from modal resubmit), extract name from it
    if params:
        params_dict = json.loads(params)
        name = params_dict.get("name", name)

    # First call (no PIN): show PIN modal
    if pin is None:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/olympiads",
                "params": {"name": name},
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        return response

    # PIN provided but invalid
    if len(pin) != 4:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/olympiads",
                "params": {"name": name},
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
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    # Happy path - append new olympiad to the list

    conn.execute(
        f"INSERT INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
        (request.state.session_id, olympiad_id)
    )
    conn.commit()

    item = {"id": olympiad_id, "name": name, "version": 1}
    response = HTMLResponse(
        templates.get_template("entity_element.html").render(
            item=item, entities="olympiads", hx_target="#olympiad-badge-container"
        )
    )
    response.headers["HX-Retarget"] = "#entity-list"
    response.headers["HX-Reswap"] = "beforeend"
    return response

def is_session_authenticated(session_id, session_data, olympiad_id, conn):
    auth_row = conn.execute(
        "SELECT olympiad_id FROM session_olympiad_auth WHERE session_id = ? AND olympiad_id = ?", 
        (session_id, olympiad_id)
    ).fetchone()

    if auth_row:
        return AuthResult.AUTHENTICATED
    else:
        olympiad_row = conn.execute(f"SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
        if olympiad_row:
            return AuthResult.NOT_AUTHENTICATED
        else:
            if session_data["selected_olympiad_id"] == olympiad_id:
                conn.execute(
                    "UPDATE sessions SET (selected_olympiad_id, selected_olympiad_version) = (NULL, NULL) WHERE id = ?",
                    (session_id,)
                )
                conn.commit()
            return AuthResult.OLYMPIAD_DELETED


@pin_protected("rename_olympiad")
def _rename_olympiad(request, olympiad_id, params, conn, session_id, session_data):
    olympiad_version = params["version"]
    olympiad_name = params["name"]

    olympiad_row = conn.execute(f"SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
    if not olympiad_row:
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET (selected_olympiad_id, selected_olympiad_version) = (NULL, NULL) WHERE id = ?",
                (request.state.session_id,)
            )
        response = templates.get_template("entity_deleted_oob.html").render()
    elif olympiad_row["version"] != olympiad_version:
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
                (olympiad_row["version"], session_id)
            )
        item = {"id": olympiad_id, "name": olympiad_row["name"], "version": olympiad_row["version"]}
        response = templates.get_template("entity_rename_oob.html").render(
            item=item, entities="olympiads", hx_target="#olympiad-badge-container"
        )
    else:
        try:
            conn.execute(
                f"UPDATE olympiads SET name = ?, version = version + 1 WHERE id = ? AND version = ?",
                (olympiad_name, olympiad_id, olympiad_version)
            )
        except sqlite3.IntegrityError:
            response = HTMLResponse(
                templates.get_template("olympiad_name_duplicate.html").render()
            )
            response.headers["HX-Retarget"] = "#modal-container"
            response.headers["HX-Reswap"] = "innerHTML"
            return response
        item = {"id": olympiad_id, "name": olympiad_name, "version": olympiad_version + 1}
        response = templates.get_template("entity_element.html").render(
            item=item, entities="olympiads", hx_target="#olympiad-badge-container"
        )

    conn.commit()

    response = HTMLResponse(response)
    response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
    response.headers["HX-Reswap"] = "outerHTML"
    trigger_badge_update(response)

    return response


@app.put("/api/olympiads/{id}/{version}")
async def rename_olympiad(request: Request, id: int, version: int, name: str = Form(...), conn = Depends(get_db)):
    session_id = request.state.session_id
    session_data = conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()

    params = {"version": version, "name": name}

    auth_result = is_session_authenticated(session_id, session_data, id, conn)
    if auth_result == AuthResult.AUTHENTICATED:
        return _rename_olympiad(request, id, params, conn, session_id, session_data)
    elif auth_result == AuthResult.NOT_AUTHENTICATED:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/validate_pin",
                "action_type": "rename_olympiad",
                "olympiad_id": id,
                "params": params
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response
    elif auth_result == AuthResult.OLYMPIAD_DELETED:
        response = HTMLResponse(templates.get_template("entity_deleted_oob.html").render())
        trigger_badge_update(response)
        return response
    else:
        raise Exception


@pin_protected("delete_olympiad")
def _delete_olympiad(request, olympiad_id, params, conn, session_id, session_data):
    version = params["version"]

    olympiad_row = conn.execute(f"SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    if not olympiad_row:
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                (session_id,)
            )
        response = templates.get_template("entity_deleted_oob.html").render()
    elif olympiad_row["version"] != version:
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
                (olympiad_row["version"], request.state.session_id)
            )
        item = {"id": olympiad_id, "name": olympiad_row["name"], "version": olympiad_row["version"]}
        response = templates.get_template("entity_rename_oob.html").render(
            item=item, entities="olympiads", hx_target="#main-content"
        )
    else:
        conn.execute("DELETE FROM olympiads WHERE id = ?", (olympiad_id,))
        response = templates.get_template("entity_delete.html").render()

    conn.commit()

    response = HTMLResponse(response)
    response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
    response.headers["HX-Reswap"] = "outerHTML"
    trigger_badge_update(response)

    return response


@app.delete("/api/olympiads/{id}/{version}")
async def delete_olympiad(request: Request, id: int, version: int, conn = Depends(get_db)):
    session_id = request.state.session_id
    session_data = conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()

    params = {"version": version}

    auth_result = is_session_authenticated(session_id, session_data, id, conn)
    if auth_result == AuthResult.AUTHENTICATED:
        return _delete_olympiad(request, id, params, conn, session_id, session_data)
    elif auth_result == AuthResult.NOT_AUTHENTICATED:
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/validate_pin",
                "action_type": "delete_olympiad",
                "olympiad_id": id,
                "params": params
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response
    elif auth_result == AuthResult.OLYMPIAD_DELETED:
        response = HTMLResponse(templates.get_template("entity_deleted_oob.html").render())
        trigger_badge_update(response)
        return response
    else:
        raise Exception


@app.post("/api/validate_pin")
async def validate_pin(
    request: Request,
    pin: str = Form(...),
    action_type: str = Form(...),
    olympiad_id: int = Form(...),
    params: str = Form("{}"),
    conn = Depends(get_db)
):
    session_id = request.state.session_id

    # Get session data for the action handler
    session_data = conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()

    parsed_params = json.loads(params)

    # Get the olympiad and its PIN
    olympiad_row = conn.execute("SELECT id, pin FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    if not olympiad_row:
        # Olympiad was deleted while PIN modal was open
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET (selected_olympiad_id, selected_olympiad_version) = (NULL, NULL) WHERE id = ?",
                (request.state.session_id,)
            )
            conn.commit()
        # If the session was doing stuff on the olympiads page this makes sense, otherwise we must go with the big modal
        if action_type == "rename_olympiad" or action_type == "delete_olympiad":
            response = HTMLResponse(templates.get_template("entity_deleted_oob.html").render())
            response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
            response.headers["HX-Reswap"] = "outerHTML"
            response.headers["HX-Trigger-After-Settle"] = "closeModal"
            trigger_badge_update(response)

            return response
        else:
            response = HTMLResponse(templates.get_template("entity_not_found.html").render())
            response.headers["HX-Retarget"] = "#main-content"
            response.headers["HX-Trigger-After-Settle"] = "closeModal"
            trigger_badge_update(response)

            return response

    if olympiad_row["pin"] != pin:
        # Wrong PIN - show error in modal, preserve all params
        response = templates.TemplateResponse(
            request, "pin_modal.html", {
                "action": "/api/validate_pin",
                "action_type": action_type,
                "olympiad_id": olympiad_id,
                "params": parsed_params,
                "error": "PIN errato"
            }
        )
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    # PIN is correct - grant access
    conn.execute(
        "INSERT OR IGNORE INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
        (session_id, olympiad_row["id"])
    )
    conn.commit()

    # Dispatch to the registered action handler
    handler = PIN_PROTECTED_ACTIONS.get(action_type)
    if handler: return handler(request, olympiad_id, parsed_params, conn, session_id, session_data)

    # Unknown action - just close modal
    response = HTMLResponse("")
    response.headers["HX-Reswap"] = "none"
    response.headers["HX-Trigger-After-Settle"] = "closeModal"
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
