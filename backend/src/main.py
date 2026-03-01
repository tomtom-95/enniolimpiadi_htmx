import os
import json
import secrets

from contextlib import asynccontextmanager

import sqlite3

import uvicorn
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path

from . import database
from . import events

from enum import Enum

class Status(Enum):
    SUCCESS               = "success"
    OLYMPIAD_NOT_FOUND    = "olympiad_not_found"
    OLYMPIAD_RENAMED      = "olympiad_renamed"
    OLYMPIAD_NOT_SELECTED = "olympiad_not_selectec"
    NOT_AUTHORIZED        = "not_authorized"
    NAME_DUPLICATION      = "name_duplication"
    INVALID_PIN           = "invalid_pin"
    ENTITY_NOT_FOUND      = "entity_not_found"
    ENTITY_RENAMED        = "entity_renamed"

db_path = Path(os.environ["DATABASE_PATH"])
schema_path = Path(os.environ["SCHEMA_PATH"])

root = Path(os.environ["PROJECT_ROOT"])
templates = Jinja2Templates(directory=root / "frontend" / "templates")

SCORE_KINDS = [
    {"kind": "points", "label": "Punti"},
    {"kind": "outcome", "label": "Vittoria / Sconfitta"},
]

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

sentinel_olympiad_badge = {"id": 0, "name": "Olympiad badge", "version": 0}


def derive_event_status(current_stage_order: int, max_stage_order: int):
    """Derive the event's display status from current_stage_order.

    - NULL or 0 -> 'registration'
    - 1..max_stage_order -> 'started'
    - > max_stage_order -> 'finished'
    """
    if current_stage_order is None or current_stage_order == 0:
        return "registration"
    if max_stage_order is not None and current_stage_order > max_stage_order:
        return "finished"
    return "started"


def get_olympiad_from_request(request: Request) -> dict:
    """Read selected olympiad data from request headers (set by client-side JS)."""
    result = {
        "id": int(request.headers.get("X-Olympiad-Id", "0")),
        "version": int(request.headers.get("X-Olympiad-Version", "0")),
        "name": request.headers.get("X-Olympiad-Name", ""),
    }
    return result


def verify_olympiad_access(request, olympiad_id: int, olympiad_name: str):
    conn = request.state.conn
    session_id = request.state.session_id

    row = conn.execute(
        """
        SELECT 1 FROM olympiads o
        JOIN session_olympiad_auth soa ON soa.olympiad_id = o.id AND soa.session_id = ?
        WHERE o.id = ? AND o.name = ?
        """,
        (session_id, olympiad_id, olympiad_name)
    ).fetchone()
    return row is not None


def _oob_badge_html(request, olympiad_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_badge_ctx["id"],)).fetchone()

    if olympiad_id == olympiad_badge_ctx["id"]:
        if not olympiad:
            return templates.get_template("olympiad_badge.html").render(olympiad=sentinel_olympiad_badge, oob=True)
        else:
            olympiad_data = {"id": olympiad["id"], "name": olympiad["name"], "version": olympiad["version"]}
            return templates.get_template("olympiad_badge.html").render(olympiad=olympiad_data, oob=True)
    else:
        return templates.get_template("olympiad_badge.html").render(olympiad=olympiad_badge_ctx, oob=True)


def _render_access_denied(request, olympiad_id: int, olympiad_name: str):
    conn = request.state.conn

    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
    extra_headers = {}
    if not olympiad:
        html_content = templates.get_template("olympiad_not_found.html").render()
        result = Status.OLYMPIAD_NOT_FOUND
    elif olympiad["name"] != olympiad_name:
        html_content = templates.get_template("olympiad_name_changed.html").render()
        result = Status.OLYMPIAD_RENAMED
    else:
        extra_headers["HX-Pin-Required"] = "true"
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)
        result = Status.NOT_AUTHORIZED

    return result, html_content, extra_headers


def _render_operation_denied(result, olympiad_id, entities):
    html_content = ""
    extra_headers = {}

    needs_modal = (
        result == Status.OLYMPIAD_NOT_FOUND     or
        result == Status.OLYMPIAD_RENAMED       or
        result == Status.NAME_DUPLICATION       or
        result == Status.NOT_AUTHORIZED         or
        result == Status.OLYMPIAD_NOT_SELECTED
    )

    if needs_modal:
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"

    if result == Status.OLYMPIAD_NOT_SELECTED:
        html_content = templates.get_template("select_olympiad_required.html").render()
    if result == Status.OLYMPIAD_NOT_FOUND:
        html_content = templates.get_template("olympiad_not_found.html").render()
    elif result == Status.OLYMPIAD_RENAMED:
        html_content = templates.get_template("olympiad_name_changed.html").render()
    elif result == Status.NAME_DUPLICATION:
        html_content = templates.get_template("entity_name_duplicate.html").render(entities=entities)
    elif result == Status.NOT_AUTHORIZED:
        extra_headers["HX-Pin-Required"] = "true"
        html_content = templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)

    return html_content, extra_headers


def check_olympiad_exist(request: Request, olympiad_id: int):
    result = request.state.conn.execute("SELECT 1 FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
    return result


def check_entity_exist(request: Request, entities: str, entity_id: int):
    result = request.state.conn.execute(f"SELECT 1 FROM {entities} WHERE id = ?", (entity_id,)).fetchone()
    return result


def check_olympiad_name(request: Request, olympiad_id: int, olympiad_name: str):
    result = request.state.conn.execute(
        """
        SELECT 1 FROM olympiads WHERE id = ? AND name = ?
        """,
        (olympiad_id, olympiad_name)
    ).fetchone()
    return result


def check_entity_name(request: Request, entities: str, entity_id: int, entity_name: str):
    result = request.state.conn.execute(
        f"""
        SELECT 1 FROM {entities} WHERE id = ? AND name = ?
        """,
        (entity_id, entity_name)
    ).fetchone()
    return result


def check_user_authorized(request: Request, olympiad_id: int):
    session_id = request.state.session_id
    result = request.state.conn.execute(
        """
        SELECT 1 FROM olympiads o
        JOIN session_olympiad_auth soa ON soa.olympiad_id = o.id AND soa.session_id = ?
        WHERE o.id = ?
        """,
        (session_id, olympiad_id)
    ).fetchone()
    return result


def check_olympiad_name_duplication(request: Request, olympiad_id: int, name: str):
    result = request.state.conn.execute(
        """
        SELECT 1 FROM olympiads WHERE id != ? AND name = ?
        """,
        (olympiad_id, name)
    ).fetchone()
    return result


def check_entity_name_duplication(request: Request, entities: str, entity_id: int, entity_name: str):
    result = request.state.conn.execute(
        f"""
        SELECT 1 FROM {entities} WHERE id != ? AND name = ?
        """,
        (entity_id, entity_name)
    ).fetchone()
    return result


def check_pin_valid(request: Request, olympiad_id: int, pin: str):
    result = request.state.conn.execute(
        """
        SELECT 1 FROM olympiads WHERE id = ? AND pin = ?
        """,
        (olympiad_id, pin)
    ).fetchone()
    return result


def check_olympiad_auth(request, olympiad_id: int, olympiad_name: str):
    result, response, extra_headers = Status.SUCCESS, None, None

    if not verify_olympiad_access(request, olympiad_id, olympiad_name):
        result, response, extra_headers = _render_access_denied(request, olympiad_id, olympiad_name)

    return result, response, extra_headers


# def check_entity(request, entities, entity_id, entity_name):
#     conn = request.state.conn
# 
#     item = conn.execute(
#         f"SELECT * FROM {entities} WHERE id = ?",
#         (entity_id,)
#     ).fetchone()
# 
#     if not item:
#         html_content = templates.get_template("entity_deleted_oob.html").render()
#         response = HTMLResponse(html_content)
#         response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
#         response.headers["HX-Reswap"] = "outerHTML"
#         return False, response
# 
#     if item["name"] != entity_name:
#         item_data = {"id": entity_id, "name": item["name"], "version": item["version"]}
#         html_content = templates.get_template("entity_renamed_oob.html").render(
#             item=item_data, entities=entities, hx_target="#main-content"
#         )
#         response = HTMLResponse(html_content)
#         response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
#         response.headers["HX-Reswap"] = "outerHTML"
#         return False, response
# 
#     return True, None


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db(db_path, schema_path)
    database.seed_dummy_data(db_path)
    yield
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
        request.state.conn = conn

        response = await call_next(request)
        response.set_cookie("session", session_id, httponly=True, max_age=86400)
    finally:
        conn.close()

    return response


# ---------------------------------------------------------------------------
# Static / infra
# ---------------------------------------------------------------------------

@app.get("/health")
def get_health():
    return JSONResponse(200)


@app.get("/", response_class=HTMLResponse)
def read_root():
    """Serve the main HTML file"""
    html_path = root / "frontend" / "index.html"
    return HTMLResponse(content=html_path.read_text())


@app.get("/index.css")
def serve_css():
    """Serve CSS files from frontend directory"""
    css_path = root / "frontend" / f"index.css"
    return Response(content=css_path.read_text(), media_type="text/css")


# ---------------------------------------------------------------------------
# Olympiad Specific Routes
# ---------------------------------------------------------------------------

@app.get("/api/olympiads")
def list_olympiads(request: Request):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    result = Status.SUCCESS
    if olympiad_badge_ctx["id"] != 0:
        if not check_olympiad_exist(request, olympiad_id):
            result = Status.OLYMPIAD_NOT_FOUND
        
        if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
            result = Status.OLYMPIAD_RENAMED
        

    extra_headers = {}
    if result == Status.OLYMPIAD_NOT_FOUND:
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("olympiad_not_found.html").render()
    elif result == Status.OLYMPIAD_RENAMED:
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("olympiad_name_changed.html").render()
    else:
        placeholder = "Aggiungi un olympiade"
        cursor = conn.execute("SELECT id, name, version FROM olympiads")
        rows = [
            { "id": row["id"], "name": row["name"], "version": row["version"] }
            for row in cursor.fetchall()
        ]
        html_content = templates.get_template("entity_list.html")
        html_content = html_content.render(entities="olympiads", placeholder=placeholder, items=rows)
    
    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response



@app.get("/api/olympiads/create")
def get_create_olympiad_modal(request: Request, name: str = Query(...)):
    template_ctx = { "params": { "name": name } }
    return templates.TemplateResponse(request, "pin_modal.html", template_ctx)


@app.post("/api/olympiads")
def create_olympiad(request: Request, pin: str = Form(...), name: str = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if check_olympiad_name_duplication(request, 0, name):
        result = Status.NAME_DUPLICATION
    
    if result == Status.SUCCESS and len(pin) != 4:
        result = Status.INVALID_PIN

    extra_headers = {}
    if result == Status.INVALID_PIN:
        error_message = "Il PIN deve essere composto da 4 cifre"
        html_content = templates.get_template("pin_modal.html")
        html_content = html_content.render(params={ "name": name }, error=error_message)
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
    if result == Status.NAME_DUPLICATION:
        html_content = templates.get_template("olympiad_name_duplicate.html").render()
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
    else:
        row = conn.execute(
            f"INSERT INTO olympiads (name, pin) VALUES (?, ?) RETURNING id", (name, pin)
        ).fetchone()
        olympiad_id = row[0]

        conn.execute(
            f"INSERT INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
            (session_id, olympiad_id)
        )
        item = {"id": olympiad_id, "name": name}
        html_content = templates.get_template("entity_element.html")
        html_content = html_content.render(item=item, entities="olympiads", hx_target=f"#olympiads-{olympiad_id}")

        # Maybe a bit ugly to inline the html like this but actually it does its job really well
        html_content += '<div id="modal-container" hx-swap-oob="innerHTML"></div>'


    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response


@app.get("/api/olympiads/{olympiad_id}")
def select_olympiad(request: Request, olympiad_id: int, olympiad_name: str = Query(..., alias="name")):
    conn = request.state.conn

    hx_target = f"#{request.headers.get('HX-Target')}"

    olympiad_badge_ctx = get_olympiad_from_request(request)

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED


    if result == Status.OLYMPIAD_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
        html_content += _oob_badge_html(request, olympiad_badge_ctx["id"])
    else:
        olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
        olympiad_data = { "id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"] }
        if result == Status.OLYMPIAD_RENAMED:
            html_content = templates.get_template("entity_renamed_oob.html")
            html_content = html_content.render(entities="olympiads", item=olympiad_data, hx_target=hx_target)
            html_content += _oob_badge_html(request, olympiad_data["id"])
        else:
            html_content = templates.get_template("entity_element.html")
            html_content = html_content.render(entities="olympiads", item=olympiad_data, hx_target=hx_target)
            html_content += templates.get_template("olympiad_badge.html").render(olympiad=olympiad_data, oob=True)


    response = HTMLResponse(html_content)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response


def _get_edit_textbox(request: Request, entities: str, item_id: int, name: str):
    template_ctx = { "curr_name": name, "entities": entities, "id": item_id }
    return templates.TemplateResponse(request, "edit_entity.html", template_ctx)

@app.get("/api/olympiads/{item_id}/edit")
def get_edit_textbox_olympiads(request: Request, item_id: int, name: str = Query(...)):
    return _get_edit_textbox(request, "olympiads", item_id, name)

@app.get("/api/players/{item_id}/edit")
def get_edit_textbox_players(request: Request, item_id: int, name: str = Query(...)):
    return _get_edit_textbox(request, "players", item_id, name)

@app.get("/api/teams/{item_id}/edit")
def get_edit_textbox_teams(request: Request, item_id: int, name: str = Query(...)):
    return _get_edit_textbox(request, "teams", item_id, name)

@app.get("/api/events/{item_id}/edit")
def get_edit_textbox_events(request: Request, item_id: int, name: str = Query(...)):
    return _get_edit_textbox(request, "events", item_id, name)


def _cancel_edit(request: Request, entities: str, item_id: int, name: str):
    hx_target = "#olympiad-badge" if entities == "olympiads" else "#main-content"
    template_ctx = {"item": { "id": item_id, "name": name }, "entities": entities, "hx_target": hx_target }
    return templates.TemplateResponse(request, "entity_element.html", template_ctx)

@app.get("/api/olympiads/{item_id}/cancel-edit")
def cancel_edit_olympiads(request: Request, item_id: int, name: str = Query(...)):
    return _cancel_edit(request, "olympiads", item_id, name)

@app.get("/api/players/{item_id}/cancel-edit")
def cancel_edit_players(request: Request, item_id: int, name: str = Query(...)):
    return _cancel_edit(request, "players", item_id, name)

@app.get("/api/teams/{item_id}/cancel-edit")
def cancel_edit_teams(request: Request, item_id: int, name: str = Query(...)):
    return _cancel_edit(request, "teams", item_id, name)

@app.get("/api/events/{item_id}/cancel-edit")
def cancel_edit_events(request: Request, item_id: int, name: str = Query(...)):
    return _cancel_edit(request, "events", item_id, name)


@app.put("/api/olympiads/{olympiad_id}")
def rename_olympiad(
    request: Request,
    olympiad_id: int,
    olympiad_curr_name: str = Form(..., alias="curr_name"),
    olympiad_new_name: str = Form(..., alias="new_name")
):
    assert olympiad_id != 0

    conn = request.state.conn
    conn.execute("BEGIN IMMEDIATE")

    hx_target = f"#{request.headers.get('HX-Target')}"

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_curr_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and check_olympiad_name_duplication(request, olympiad_id, olympiad_new_name):
        result = Status.NAME_DUPLICATION
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    extra_headers = {}
    if result == Status.OLYMPIAD_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.OLYMPIAD_RENAMED:
        olympiad_badge_ctx = { "id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"] }
        html_content = templates.get_template("entity_renamed_oob.html")
        html_content = html_content.render(entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)
    elif result == Status.NOT_AUTHORIZED:
        extra_headers["HX-Pin-Required"] = "true"
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)
    elif result == Status.NAME_DUPLICATION:
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("olympiad_name_duplicate.html").render()
    else:
        updated_row = conn.execute(
            "UPDATE olympiads SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (olympiad_new_name, olympiad_id)
        ).fetchone()
        item = {"id": olympiad_id, "name": updated_row["name"]}
        html_content = templates.get_template("entity_element.html")
        html_content = html_content.render(item=item, entities="olympiads", hx_target=hx_target)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)
    
    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response


@app.delete("/api/olympiads/{olympiad_id}")
def delete_olympiad(request: Request, olympiad_id: int, olympiad_name: str = Query(..., alias="name")):
    conn = request.state.conn
    conn.execute("BEGIN IMMEDIATE")

    hx_target = f"#{request.headers.get('HX-Target')}"

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    extra_headers = {}
    if result == Status.OLYMPIAD_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.OLYMPIAD_RENAMED:
        olympiad_badge_ctx = { "id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"] }
        html_content = templates.get_template("entity_renamed_oob.html")
        html_content = html_content.render(entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)
    elif result == Status.NOT_AUTHORIZED:
        extra_headers["HX-Pin-Required"] = "true"
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)
    else:
        conn.execute(
            "DELETE FROM olympiads WHERE id = ? AND name = ? RETURNING id",
            (olympiad_id, olympiad_name)
        ).fetchone()
        html_content = templates.get_template("entity_delete.html").render()

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response


# ---------------------------------------------------------------------------
# Entity helpers - shared logic
# ---------------------------------------------------------------------------

def _list_entities(request: Request, entities: str):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    result = Status.SUCCESS
    if olympiad_id == 0:
        result = Status.OLYMPIAD_NOT_SELECTED
    if result == Status.SUCCESS and not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, entities)

    if result == Status.SUCCESS:
        items = conn.execute(
            f"SELECT e.id, e.name, e.version FROM {entities} e WHERE e.olympiad_id = ?",
            (olympiad_id,)
        ).fetchall()
        placeholder = entity_list_form_placeholder[entities]
        html_content = templates.get_template("entity_list.html")
        html_content = html_content.render(entities=entities, placeholder=placeholder, items=items)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response

@app.get("/api/players")
def list_players(request: Request):
    return _list_entities(request, "players")

@app.get("/api/teams")
def list_teams(request: Request):
    return _list_entities(request, "teams")

@app.get("/api/events")
def list_events(request: Request):
    return _list_entities(request, "events")


@app.post("/api/players")
def create_player(request: Request, name: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if olympiad_id == 0:
        result = Status.OLYMPIAD_NOT_SELECTED
    if result == Status.SUCCESS and not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and check_entity_name_duplication(request, "players", 0, name):
        result = Status.NAME_DUPLICATION
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "players")

    if result == Status.SUCCESS:
        inserted_row = conn.execute(
            "INSERT INTO players (name, olympiad_id) VALUES (?, ?) RETURNING id, name, version",
            (name, olympiad_id)
        ).fetchone()

        conn.execute(
            "INSERT INTO participants (player_id, team_id) VALUES (?, ?)",
            (inserted_row["id"], None)
        )

        item = {"id": inserted_row["id"], "name": inserted_row["name"], "version": inserted_row["version"]}
        html_content = templates.get_template("entity_element.html").render(
            item=item, entities="players", hx_target="#main-content"
        )
        extra_headers["HX-Retarget"] = "#entity-list"
        extra_headers["HX-Reswap"] = "afterbegin"

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.post("/api/teams")
def create_team(request: Request, name: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if olympiad_id == 0:
        result = Status.OLYMPIAD_NOT_SELECTED
    if result == Status.SUCCESS and not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and check_entity_name_duplication(request, "teams", 0, name):
        result = Status.NAME_DUPLICATION
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "teams")

    if result == Status.SUCCESS:
        inserted_row = conn.execute(
            "INSERT INTO teams (name, olympiad_id) VALUES (?, ?) RETURNING id, name, version",
            (name, olympiad_id)
        ).fetchone()

        conn.execute(
            "INSERT INTO participants (player_id, team_id) VALUES (?, ?)",
            (None, inserted_row["id"])
        )

        item = {"id": inserted_row["id"], "name": inserted_row["name"], "version": inserted_row["version"]}
        html_content = templates.get_template("entity_element.html").render(
            item=item, entities="teams", hx_target="#main-content"
        )
        extra_headers["HX-Retarget"] = "#entity-list"
        extra_headers["HX-Reswap"] = "afterbegin"

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.post("/api/events")
def create_event(request: Request, name: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and check_entity_name_duplication(request, "events", 0, name):
        result = Status.NAME_DUPLICATION
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        event = conn.execute(
            "INSERT INTO events (name, olympiad_id, score_kind) VALUES (?, ?, 'points') RETURNING id, name, version",
            (name, olympiad_id)
        ).fetchone()
        event_data = {"id": event["id"], "name": event["name"], "version": event["version"], "status": "registration"}
        html_content = templates.get_template("event_page.html")
        html_content = html_content.render(event=event_data)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response


def _rename_entity(request: Request, entities: str, entity_id: int, entity_curr_name: str, entity_new_name: str):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    hx_target = f"#{request.headers.get('HX-Target')}"

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, entities, entity_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_entity_name(request, entities, entity_id, entity_curr_name):
        result = Status.ENTITY_RENAMED
    if result == Status.SUCCESS and check_entity_name_duplication(request, entities, 0, entity_new_name):
        result = Status.NAME_DUPLICATION
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, entities)

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.ENTITY_RENAMED:
        entity = conn.execute(f"SELECT * FROM {entities} WHERE id = ?", (entity_id,)).fetchone()
        entity_data = { "id": entity_id, "name": entity["name"], "version": entity["version"] }
        html_content = templates.get_template("entity_renamed_oob.html")
        html_content = html_content.render(entities=entities, item=entity_data, hx_target=hx_target)
    elif result == Status.SUCCESS:
        updated_row = conn.execute(
            f"UPDATE {entities} SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (entity_new_name, entity_id)
        ).fetchone()
        item = {"id": entity_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = templates.get_template("entity_element.html")
        html_content = html_content.render(item=item, entities=entities, hx_target=hx_target)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)
    
    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response

@app.put("/api/players/{entity_id}")
def rename_players(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    return _rename_entity(request, "players", entity_id, curr_name, new_name)

@app.put("/api/teams/{entity_id}")
def rename_teams(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    return _rename_entity(request, "teams", entity_id, curr_name, new_name)

@app.put("/api/events/{entity_id}")
def rename_events(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    return _rename_entity(request, "events", entity_id, curr_name, new_name)


def _delete_entity(request: Request, entities: str, entity_id: int, entity_name: str):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    hx_target = f"#{request.headers.get('HX-Target')}"

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, entities, entity_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_entity_name(request, entities, entity_id, entity_name):
        result = Status.ENTITY_RENAMED
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, entities)

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.ENTITY_RENAMED:
        entity = conn.execute(f"SELECT * FROM {entities} WHERE id = ?", (entity_id,)).fetchone()
        entity_data = { "id": entity_id, "name": entity["name"], "version": entity["version"] }
        html_content = templates.get_template("entity_renamed_oob.html")
        html_content = html_content.render(entities=entities, item=entity_data, hx_target=hx_target)
    elif result == Status.SUCCESS:
        conn.execute(f"DELETE FROM {entities} WHERE id = ?", (entity_id,))
        html_content = templates.get_template("entity_delete.html").render()

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)
    
    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response

@app.delete("/api/players/{entity_id}")
def delete_players(request: Request, entity_id: int, entity_name: str = Query(..., alias="name")):
    return _delete_entity(request, "players", entity_id, entity_name)

@app.delete("/api/teams/{entity_id}")
def delete_teams(request: Request, entity_id: int, entity_name: str = Query(..., alias="name")):
    return _delete_entity(request, "teams", entity_id, entity_name)

@app.delete("/api/events/{entity_id}")
def delete_events(request: Request, entity_id: int, entity_name: str = Query(..., alias="name")):
    return _delete_entity(request, "events", entity_id, entity_name)


# ---------------------------------------------------------------------------
# Event-specific routes
# ---------------------------------------------------------------------------

@app.get("/api/events/{event_id}")
def select_event(request: Request, event_id: int, event_name: str = Query(..., alias="name")):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    hx_target = f"#{request.headers.get('HX-Target')}"

    assert olympiad_id != 0

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_entity_name(request, "events", event_id, event_name):
        result = Status.ENTITY_RENAMED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.ENTITY_RENAMED:
        event = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        event_data = {"id": event_id, "name": event["name"], "version": event["version"]}
        html_content = templates.get_template("entity_renamed_oob.html")
        html_content = html_content.render(entities="events", item=event_data, hx_target=hx_target)
    elif result == Status.SUCCESS:
        max_stage = conn.execute(
            "SELECT MAX(stage_order) AS max_order FROM event_stages WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        max_stage_order = max_stage["max_order"] if max_stage else None

        event = conn.execute(
            "SELECT id, name, version, current_stage_order, score_kind FROM events WHERE id = ?",
            (event_id,)
        ).fetchone()
        event_status = derive_event_status(event["current_stage_order"], max_stage_order)
        event_data = { "id": event["id"], "name": event["name"], "version": event["version"], "status": event_status }
        html_content = templates.get_template("event_page.html")
        html_content = html_content.render(event=event_data)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


def _set_event_stage_order(request: Request, event_id: int, new_stage_order: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        conn.execute(
            "UPDATE events SET current_stage_order = ? WHERE id = ?",
            (new_stage_order, event_id)
        )

        max_stage = conn.execute(
            "SELECT MAX(stage_order) AS max_order FROM event_stages WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        max_stage_order = max_stage["max_order"] if max_stage else None

        event = conn.execute(
            "SELECT id, name, version FROM events WHERE id = ?",
            (event_id,)
        ).fetchone()
        event_status = derive_event_status(new_stage_order, max_stage_order)
        event_data = {"id": event["id"], "name": event["name"], "version": event["version"], "status": event_status}
        html_content = templates.get_template("event_page.html").render(event=event_data)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.post("/api/events/{event_id}/start")
def start_event(request: Request, event_id: int):
    return _set_event_stage_order(request, event_id, 1)


@app.post("/api/events/{event_id}/back-to-registration")
def back_to_registration(request: Request, event_id: int):
    return _set_event_stage_order(request, event_id, 0)


@app.get("/api/events/{event_id}/players")
def get_event_players(request: Request, event_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    
    # TODO: should I also look for event name changed? I think so

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        enrolled_participants = conn.execute(
            """
            SELECT ep.participant_id AS id, COALESCE(pl.name, t.name) AS name
            FROM event_participants ep
            JOIN participants p ON p.id = ep.participant_id
            LEFT JOIN players pl ON pl.id = p.player_id
            LEFT JOIN teams t ON t.id = p.team_id
            WHERE ep.event_id = ?
            ORDER BY name
            """,
            (event_id,)
        ).fetchall()
        enrolled_ids = { p["id"] for p in enrolled_participants }

        all_participants = conn.execute(
            """
            SELECT p.id, COALESCE(pl.name, t.name) AS name
            FROM participants p
            LEFT JOIN players pl ON pl.id = p.player_id
            LEFT JOIN teams t ON t.id = p.team_id
            WHERE COALESCE(pl.olympiad_id, t.olympiad_id) = ?
            ORDER BY name
            """,
            (olympiad_id,)
        ).fetchall()
        available_participants = [ p for p in all_participants if p["id"] not in enrolled_ids ]
        html_content = templates.get_template("event_players_section.html")
        html_content = html_content.render(event_id=event_id, enrolled_participants=enrolled_participants, available_participants=available_participants)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


@app.get("/api/events/{event_id}/stage/{stage_order}")
def get_event_stage(request: Request, event_id: int, stage_order: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("event_not_found.html").render()
    elif result == Status.SUCCESS:
        row = conn.execute(
            """
            SELECT e.current_stage_order, es.id, es.stage_order, es.kind, sk.label
            FROM events e
            LEFT JOIN event_stages es ON es.event_id = e.id AND es.stage_order = ?
            LEFT JOIN stage_kinds sk ON sk.kind = es.kind
            WHERE e.id = ?
            """,
            (stage_order, event_id)
        ).fetchone()

        if not row or row["id"] is None:
            html_content = "<div class='error-banner'>Fase non trovata</div>"
        else:
            stage_id    = row["id"]
            stage_kind  = row["kind"]
            stage_label = row["label"]

            total_stages = conn.execute(
                "SELECT COUNT(*) AS count FROM event_stages WHERE event_id = ?",
                (event_id,)
            ).fetchone()["count"]

            if stage_kind == "groups":
                stage = events.present_groups_stage(conn, stage_id)
            elif stage_kind == "round_robin":
                html_content = "<div class='error-banner'>Fase non trovata</div>"
            elif stage_kind == "single_elimination":
                stage = events.present_single_elimination_stage(conn, stage_id)

            if stage_kind != "round_robin":
                stage["name"] = stage_label
                html_content = templates.get_template("event_stage.html").render(
                    stage=stage,
                    stage_kind=stage_kind,
                    stage_order=stage_order,
                    total_stages=total_stages,
                    event_id=event_id,
                    stage_id=stage_id,
                )

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


@app.post("/api/events/{event_id}/stages/{stage_id}/resize")
def resize_stage_groups(request: Request, event_id: int, stage_id: int, num_groups: int = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        event_row = conn.execute(
            "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if not event_row or event_row["current_stage_order"] != 0:
            html_content = "<div class='error-banner'>Resize consentito solo durante la registrazione</div>"
        else:
            stage_row = conn.execute(
                "SELECT id, stage_order, kind FROM event_stages WHERE id = ? AND event_id = ?",
                (stage_id, event_id)
            ).fetchone()
            if not stage_row or stage_row["kind"] not in ("groups", "round_robin"):
                html_content = "<div class='error-banner'>Fase non trovata o già iniziata</div>"
            else:
                total = conn.execute(
                    "SELECT COUNT(*) as c FROM event_participants WHERE event_id = ?",
                    (event_id,)
                ).fetchone()["c"]

                if total < 2:
                    html_content = "<div class='error-banner'>Servono almeno 2 partecipanti</div>"
                else:
                    events.generate_groups_stage(conn, stage_id, num_groups)
                    stage = events.present_groups_stage(conn, stage_id)
                    html_content = templates.get_template("stage_groups.html").render(
                        stage=stage, event_id=event_id,
                    )

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Event live-editing endpoints
# ---------------------------------------------------------------------------

@app.put("/api/events/{event_id}/score_kind")
def update_event_score_kind(request: Request, event_id: int, score_kind: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        conn.execute("UPDATE events SET score_kind = ? WHERE id = ?", (score_kind, event_id))
        html_content = templates.get_template("score_kind.html").render(
            event_id=event_id, score_kinds=SCORE_KINDS, current_score_kind=score_kind,
        )
        extra_headers["HX-Retarget"] = "#score-kind-section"
        extra_headers["HX-Reswap"] = "outerHTML"

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.get("/api/events/{event_id}/setup")
def get_event_setup(request: Request, event_id: int, version: int = Query(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        current_score_kind = conn.execute(
            "SELECT score_kind FROM events WHERE id = ?", (event_id,)
        ).fetchone()["score_kind"]

        stage_kinds = conn.execute(
            "SELECT kind, label FROM stage_kinds ORDER BY kind"
        ).fetchall()

        stages = conn.execute(
            "SELECT es.id, es.stage_order, es.kind, sk.label "
            "FROM event_stages es JOIN stage_kinds sk ON sk.kind = es.kind "
            "WHERE es.event_id = ? ORDER BY es.stage_order",
            (event_id,)
        ).fetchall()

        html_content = templates.get_template("event_setup.html").render(
            event_id=event_id,
            score_kinds=SCORE_KINDS,
            current_score_kind=current_score_kind,
            stage_kinds=stage_kinds,
            stages=stages,
        )

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


@app.post("/api/events/{event_id}/stages")
def add_event_stage(request: Request, event_id: int, kind: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        max_order = conn.execute(
            "SELECT COALESCE(MAX(stage_order), 0) AS m FROM event_stages WHERE event_id = ?",
            (event_id,)
        ).fetchone()["m"]

        stage_order = max_order + 1
        stage_id = conn.execute(
            "INSERT INTO event_stages (event_id, kind, stage_order) VALUES (?, ?, ?) RETURNING id",
            (event_id, kind, stage_order)
        ).fetchone()["id"]

        if stage_order == 1:
            if kind == "groups":
                events.generate_groups_stage(conn, stage_id, 1)
            elif kind == "single_elimination":
                events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_stages_section_html(conn, event_id)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.delete("/api/events/{event_id}/stages/{stage_id}")
def remove_event_stage(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        conn.execute("DELETE FROM event_stages WHERE id = ? AND event_id = ?", (stage_id, event_id))

        remaining = conn.execute(
            "SELECT id, kind FROM event_stages WHERE event_id = ? ORDER BY stage_order",
            (event_id,)
        ).fetchall()
        for i, row in enumerate(remaining):
            conn.execute(
                "UPDATE event_stages SET stage_order = ? WHERE id = ?",
                (i + 1, row["id"])
            )
            if i == 0: 
                stage_kind = row["kind"]
                stage_id = row["id"]
                if stage_kind == "groups":
                    groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                    events.generate_groups_stage(conn, stage_id, len(groups))
                elif stage_kind == "single_elimination":
                    events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_stages_section_html(conn, event_id)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


def _render_stages_section_html(conn, event_id):
    """Re-render the stages setup section for the event page."""
    stage_kinds = conn.execute(
        "SELECT kind, label FROM stage_kinds ORDER BY kind"
    ).fetchall()

    stages = conn.execute(
        """
        SELECT es.id, es.stage_order, es.kind, sk.label
        FROM event_stages es JOIN stage_kinds sk ON sk.kind = es.kind
        WHERE es.event_id = ? ORDER BY es.stage_order
        """,
        (event_id,)
    ).fetchall()

    html_content = templates.get_template("event_stages_setup.html")
    html_content = html_content.render(event_id=event_id, stage_kinds=stage_kinds, stages=stages)

    return html_content


def _render_event_players_section_html(conn, event_id, olympiad_id):
    enrolled_participants = conn.execute(
        """
        SELECT ep.participant_id AS id, COALESCE(pl.name, t.name) AS name
        FROM event_participants ep
        JOIN participants p ON p.id = ep.participant_id
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        WHERE ep.event_id = ?
        ORDER BY name
        """,
        (event_id,)
    ).fetchall()
    enrolled_ids = {p["id"] for p in enrolled_participants}

    all_participants = conn.execute(
        """
        SELECT p.id, COALESCE(pl.name, t.name) AS name
        FROM participants p
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        WHERE COALESCE(pl.olympiad_id, t.olympiad_id) = ?
        ORDER BY name
        """,
        (olympiad_id,)
    ).fetchall()
    available_participants = [p for p in all_participants if p["id"] not in enrolled_ids]

    return templates.get_template("event_players_section.html").render(
        event_id=event_id,
        enrolled_participants=enrolled_participants,
        available_participants=available_participants,
    )


@app.post("/api/events/{event_id}/enroll/{participant_id}")
def enroll_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        conn.execute(
            "INSERT OR IGNORE INTO event_participants (event_id, participant_id) VALUES (?, ?)",
            (event_id, participant_id)
        )

        current_stage_order = conn.execute(
            "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
        ).fetchone()["current_stage_order"]
        assert current_stage_order == 0

        first_stage = conn.execute(
            "SELECT id, kind FROM event_stages WHERE event_id = ? AND stage_order = 1",
            (event_id,)
        ).fetchone()
        if first_stage:
            stage_id   = first_stage["id"]
            stage_kind = first_stage["kind"]
            if stage_kind == "groups":
                groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                events.generate_groups_stage(conn, stage_id, len(groups))
            elif stage_kind == "single_elimination":
                events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_event_players_section_html(conn, event_id, olympiad_id)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.delete("/api/events/{event_id}/enroll/{participant_id}")
def unenroll_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
        result = Status.OLYMPIAD_RENAMED
    if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
        result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.SUCCESS:
        conn.execute(
            "DELETE FROM event_participants WHERE event_id = ? AND participant_id = ?",
            (event_id, participant_id)
        )

        current_stage_order = conn.execute(
            "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
        ).fetchone()["current_stage_order"]
        assert current_stage_order == 0

        first_stage = conn.execute(
            "SELECT id, kind FROM event_stages WHERE event_id = ? AND stage_order = 1",
            (event_id,)
        ).fetchone()
        if first_stage:
            stage_id   = first_stage["id"]
            stage_kind = first_stage["kind"]
            if stage_kind == "groups":
                groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                events.generate_groups_stage(conn, stage_id, len(groups))
            elif stage_kind == "single_elimination":
                events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_event_players_section_html(conn, event_id, olympiad_id)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.post("/api/validate_pin")
def validate_pin(request: Request, pin: str = Form(...), olympiad_id: int = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_olympiad_exist(request, olympiad_id):
        result = Status.OLYMPIAD_NOT_FOUND
    
    if result == Status.SUCCESS and not check_pin_valid(request, olympiad_id, pin):
        result = Status.INVALID_PIN
    

    extra_headers = {}
    if result == Status.OLYMPIAD_NOT_FOUND:
        html_content = templates.get_template("olympiad_not_found.html").render()
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
    elif result == Status.INVALID_PIN:
        html_content = templates.get_template("pin_modal.html")
        html_content = html_content.render(olympiad_id=olympiad_id, error="PIN errato")
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
    else:
        conn.execute(
            """
            INSERT INTO session_olympiad_auth (session_id, olympiad_id)
            VALUES (?, ?)
            """,
            (session_id, olympiad_id)
        )
        html_content = ""
        extra_headers["HX-Trigger"] = "pinValidated"
    
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()
    
    return response


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8080)
