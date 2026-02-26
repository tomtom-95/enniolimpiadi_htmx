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
    SUCCESS = "success"
    OLYMPIAD_NOT_FOUND = "olympiad_not_found"
    OLYMPIAD_RENAMED   = "olympiad_renamed"
    NOT_AUTHORIZED     = "not_authorized"

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

sentinel_olympiad_badge = {"id": 0, "name": "Olympiad Badge", "version": 0}



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
    return {
        "id": int(request.headers.get("X-Olympiad-Id", "0")),
        "version": int(request.headers.get("X-Olympiad-Version", "0")),
        "name": request.headers.get("X-Olympiad-Name", ""),
    }


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


def _modal_response(html_content):
    response = HTMLResponse(html_content)
    response.headers["HX-Retarget"] = "#modal-container"
    response.headers["HX-Reswap"] = "innerHTML"
    return response


def _oob_badge_html(request, olympiad_badge_ctx: dict):
    olympiad_data = get_olympiad_from_request(request)

    if olympiad_badge_ctx["id"] == 0 or olympiad_badge_ctx["id"] == olympiad_data["id"]:
        return templates.get_template("olympiad_badge.html").render(olympiad=olympiad_badge_ctx, oob=True)
    else:
        return templates.get_template("olympiad_badge.html").render(olympiad=olympiad_data, oob=True)


def _access_denied_response(request, olympiad_id: int, olympiad_name: str):
    conn = request.state.conn

    result = Status.SUCCESS
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
        html_content = templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)
        result = Status.NOT_AUTHORIZED

    # return result, _modal_response(html_content, extra_headers)
    return result, html_content


def check_olympiad_auth(request, olympiad_id: int, olympiad_name: str):
    if verify_olympiad_access(request, olympiad_id, olympiad_name):
        return Status.SUCCESS, None

    result, response = _access_denied_response(request, olympiad_id, olympiad_name)

    return result, response


def check_selected_olympiad(request):
    conn = request.state.conn
    selected_olympiad = get_olympiad_from_request(request)

    if selected_olympiad["id"] == 0:
        return True, None

    selected = conn.execute(
        "SELECT id, name, version FROM olympiads WHERE id = ?",
        (selected_olympiad["id"],)
    ).fetchone()

    if not selected:
        html = templates.get_template("olympiad_not_found.html").render()
        html += _oob_badge_html(request, sentinel_olympiad_badge)
        return False, _modal_response(html)
    elif selected["name"] != selected_olympiad["name"]:
        html = templates.get_template("olympiad_name_changed.html").render()
        olympiad_badge = { "id": selected["id"], "name": selected["name"], "version": selected["version"] }
        html += _oob_badge_html(request, olympiad_badge)
        return False, _modal_response(html)
    
    return True, None


def check_required_olympiad(request, entities):
    olympiad_data = get_olympiad_from_request(request)
    if olympiad_data["id"] == 0:
        template_ctx = {"message": select_olympiad_message[entities]}
        response = templates.TemplateResponse(request, "select_olympiad_required.html", template_ctx)
        return False, response
    else:
        return check_selected_olympiad(request)


# TODO: integrate event_version in verify_event_access
def verify_event_access(conn, session_id, event_id):
    """Check event exists and session is authorized. Returns olympiad_id or None."""
    row = conn.execute(
        """
        SELECT e.olympiad_id
        FROM events e
        JOIN session_olympiad_auth soa
          ON soa.olympiad_id = e.olympiad_id AND soa.session_id = ?
        WHERE e.id = ?
        """,
        (session_id, event_id)
    ).fetchone()
    return row["olympiad_id"] if row else None


def check_entity(request, entities, entity_id, entity_name):
    conn = request.state.conn

    item = conn.execute(
        f"SELECT * FROM {entities} WHERE id = ?",
        (entity_id,)
    ).fetchone()

    if not item:
        html_content = templates.get_template("entity_deleted_oob.html").render()
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        return False, response

    if item["name"] != entity_name:
        item_data = {"id": entity_id, "name": item["name"], "version": item["version"]}
        html_content = templates.get_template("entity_renamed_oob.html").render(
            item=item_data, entities=entities, hx_target="#main-content"
        )
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        return False, response

    return True, None


def diagnose_event_noop(
    request: Request,
    event_id: int,
    session_id: str,
) -> Response:
    """
    Diagnose why an event operation was a no-op and return appropriate response.

    Called after verify_event_access returned None.
    Checks: event deleted, olympiad deleted, or not authorized.
    """
    conn = request.state.conn

    event = conn.execute(
        "SELECT olympiad_id FROM events WHERE id = ?", (event_id,)
    ).fetchone()

    if not event:
        # Event gone - check if the olympiad is gone too
        olympiad_data = get_olympiad_from_request(request)
        olympiad_id = olympiad_data["id"] if olympiad_data else None
        olympiad_exists = conn.execute(
            "SELECT id FROM olympiads WHERE id = ?", (olympiad_id,)
        ).fetchone() if olympiad_id else None

        if not olympiad_exists:
            response = templates.TemplateResponse(request, "olympiad_not_found.html")
            response.headers["HX-Retarget"] = "#main-content"
            response.headers["HX-Reswap"] = "innerHTML"
            trigger_badge_update(response)
            return response

        # Olympiad exists but event was deleted
        response = templates.TemplateResponse(request, "event_not_found.html")
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    # Event exists but not authorized - show PIN modal
    response = templates.TemplateResponse(
        request, "pin_modal.html", {"olympiad_id": event["olympiad_id"]}
    )
    response.headers["HX-Pin-Required"] = "true"
    response.headers["HX-Retarget"] = "#modal-container"
    response.headers["HX-Reswap"] = "innerHTML"
    return response



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
    res, response = check_selected_olympiad(request)
    if not res:
        return response

    cursor = request.state.conn.execute("SELECT id, name, version FROM olympiads")
    rows = [
        { "id": row["id"], "name": row["name"], "version": row["version"] }
        for row in cursor.fetchall()
    ]
    template_ctx = { "entities": "olympiads", "placeholder": "Aggiungi un olimpiade", "items": rows }
    return templates.TemplateResponse(request, "entity_list.html", template_ctx)


@app.get("/api/olympiads/create")
def get_create_olympiad_modal(request: Request, name: str = Query(...)):
    template_ctx = { "params": { "name": name } }
    return templates.TemplateResponse(request, "pin_modal.html", template_ctx)


@app.post("/api/olympiads")
def create_olympiad(request: Request, pin: str = Form(...), name: str = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    if len(pin) != 4:
        template_ctx = { "params": { "name": name }, "error": "Il PIN deve essere composto da 4 cifre" }
        response = templates.TemplateResponse(request, "pin_modal.html", template_ctx)
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response
    try:
        row = conn.execute(
            f"INSERT INTO olympiads (name, pin) VALUES (?, ?) RETURNING id", (name, pin)
        ).fetchone()
        olympiad_id = row[0]
    except sqlite3.IntegrityError:
        response = templates.TemplateResponse(request, "olympiad_name_duplicate.html")
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    conn.execute(
        f"INSERT INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
        (session_id, olympiad_id)
    )
    conn.commit()

    item = { "id": olympiad_id, "name": name }
    entity_html = templates.get_template("entity_element.html").render(
        item=item, entities="olympiads", hx_target="#olympiad-badge"
    )
    entity_html += '<div id="modal-container" hx-swap-oob="innerHTML"></div>'
    response = HTMLResponse(entity_html)
    response.headers["HX-Retarget"] = "#entity-list"
    response.headers["HX-Reswap"] = "afterbegin"
    return response


@app.get("/api/olympiads/{olympiad_id}")
def select_olympiad(request: Request, olympiad_id: int, olympiad_name: str = Query(..., alias="name")):
    """Select an olympiad and update the olympiad badge"""
    olympiad = request.state.conn.execute(
        "SELECT id, name, version FROM olympiads WHERE id = ?",
        (olympiad_id,)
    ).fetchone()

    hx_target = f"#{request.headers.get('HX-Target')}"

    if not olympiad:
        olympiad_badge_ctx = sentinel_olympiad_badge
        html_content = templates.get_template("entity_deleted_oob.html").render()
    else:
        olympiad_badge_ctx = {"id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"]}
        if olympiad["name"] != olympiad_name:
            html_content = templates.get_template("entity_renamed_oob.html")
            html_content = html_content.render(entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)
        else:
            html_content = templates.get_template("entity_element.html")
            html_content = html_content.render(entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)
    
    html_content += templates.get_template("olympiad_badge.html").render(olympiad=olympiad_badge_ctx, oob=True)

    response = HTMLResponse(html_content)

    return response


def _get_edit_textbox(request: Request, entities: str, item_id: int, name: str):
    template_ctx = { "curr_name": name, "entities": entities, "id": item_id }
    return templates.TemplateResponse(request, "edit_entity.html", template_ctx)


def _cancel_edit(request: Request, entities: str, item_id: int, name: str):
    hx_target = "#olympiad-badge" if entities == "olympiads" else "#main-content"
    template_ctx = {"item": { "id": item_id, "name": name }, "entities": entities, "hx_target": hx_target }
    return templates.TemplateResponse(request, "entity_element.html", template_ctx)


@app.put("/api/olympiads/{olympiad_id}")
def rename_olympiad(
    request: Request,
    olympiad_id: int,
    olympiad_curr_name: str = Form(..., alias="curr_name"),
    olympiad_new_name: str = Form(..., alias="new_name")
):
    conn = request.state.conn
    conn.execute("BEGIN IMMEDIATE")

    hx_target = f"#{request.headers.get('HX-Target')}"

    olympiad_badge_ctx = get_olympiad_from_request(request)
    result, html_content = check_olympiad_auth(request, olympiad_id, olympiad_curr_name)

    extra_headers = {}
    if result == Status.SUCCESS:
        duplicate = conn.execute(
            "SELECT 1 FROM olympiads WHERE name = ? AND id != ?",
            (olympiad_new_name, olympiad_id)
        ).fetchone()

        if duplicate:
            html_content = templates.get_template("olympiad_name_duplicate.html").render()
            extra_headers["HX-Retarget"] = "#modal-container"
            extra_headers["HX-Reswap"] = "innerHTML"
        else:
            updated_row = conn.execute(
                "UPDATE olympiads SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
                (olympiad_new_name, olympiad_id)
            ).fetchone()
            item = {"id": olympiad_id, "name": updated_row["name"]}
            html_content = templates.get_template("entity_element.html")
            html_content = html_content.render(item=item, entities="olympiads", hx_target=hx_target)
            olympiad_badge_ctx = { "id": olympiad_id, "name": updated_row["name"], "version": updated_row["version"] }
    elif result == Status.OLYMPIAD_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
        if olympiad_id == olympiad_badge_ctx["id"]:
            olympiad_badge_ctx = sentinel_olympiad_badge
    elif result == Status.OLYMPIAD_RENAMED:
        olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
        olympiad_badge_ctx = { "id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"] }
        html_content = templates.get_template("entity_renamed_oob.html")
        html_content = html_content.render(entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)

    html_content += _oob_badge_html(request, olympiad_badge_ctx)
    
    if result == Status.NOT_AUTHORIZED:
        response = _modal_response(html_content)
        response.headers["HX-Pin-Required"] = "true"
    else:
        response = HTMLResponse(html_content)
        if extra_headers:
            response.headers.update(extra_headers)

    if result:
        conn.commit()
    else:
        conn.rollback()
    
    return response


@app.delete("/api/olympiads/{olympiad_id}")
def delete_olympiad(request: Request, olympiad_id: int, olympiad_name: str = Query(..., alias="name")):
    conn = request.state.conn

    conn.execute("BEGIN IMMEDIATE")

    hx_target = f"#{request.headers.get('HX-Target')}"

    result, response = check_olympiad_auth(request, olympiad_id, olympiad_name)

    if result == Status.SUCCESS:
        conn.execute(
            "DELETE FROM olympiads WHERE id = ? AND name = ? RETURNING id",
            (olympiad_id, olympiad_name)
        ).fetchone()
        html_content = templates.get_template("entity_delete.html").render()
        html_content += _oob_badge_html(request, sentinel_olympiad_badge)
        response = HTMLResponse(html_content)
    elif result == Status.OLYMPIAD_NOT_FOUND:
        response = HTMLResponse(templates.get_template("entity_deleted_oob.html").render())
    elif result == Status.OLYMPIAD_RENAMED:
        html_content = templates.get_template("entity_renamed_oob.html")
        olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
        olympiad_badge_ctx = { "id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"] }
        response = HTMLResponse(html_content.render(entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target))

    if result:
        conn.commit()
    else:
        conn.rollback()
    
    return response


# ---------------------------------------------------------------------------
# Event-specific routes
# ---------------------------------------------------------------------------

@app.get("/api/events/{event_id}")
def select_event(request: Request, event_id: int, event_name: str = Query(..., alias="name")):
    res, response = check_required_olympiad(request, "events")
    if not res:
        return response

    res, response = check_entity(request, "events", event_id, event_name)
    if not res:
        return response

    conn = request.state.conn
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

    return templates.TemplateResponse(
        request,
        "event_page.html",
        {
            "event": {
                "id": event["id"],
                "name": event["name"],
                "version": event["version"],
                "status": event_status,
            },
        }
    )


@app.get("/api/events/{event_id}/players")
def get_event_players(request: Request, event_id: int):
    conn = request.state.conn
    olympiad_data = get_olympiad_from_request(request)
    olympiad_id = olympiad_data["id"] if olympiad_data else None

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
    available_participants = [
        p for p in all_participants if p["id"] not in enrolled_ids
    ]

    return templates.TemplateResponse(
        request, "event_players_section_v2.html",
        {
            "enrolled_participants": enrolled_participants,
            "available_participants": available_participants,
            "event_id": event_id,
        }
    )


@app.get("/api/events/{event_id}/stage/{stage_order}")
def get_event_stage(request: Request, event_id: int, stage_order: int):
    conn = request.state.conn
    row = conn.execute(
        """
        SELECT
            e.current_stage_order,
            es.id,
            es.stage_order,
            es.kind,
            sk.label
        FROM events e
        LEFT JOIN event_stages es ON es.event_id = e.id AND es.stage_order = ?
        LEFT JOIN stage_kinds sk ON sk.kind = es.kind
        WHERE e.id = ?
        """,
        (stage_order, event_id)
    ).fetchone()

    if not row or row["id"] is None:
        return HTMLResponse("<div class='error-banner'>Fase non trovata</div>")

    stage_id    = row["id"]
    stage_kind  = row["kind"]
    stage_label = row["label"]

    # Get total number of stages for navigation
    total_stages = conn.execute(
        "SELECT COUNT(*) AS count FROM event_stages WHERE event_id = ?",
        (event_id,)
    ).fetchone()["count"]

    if stage_kind == "groups":
        stage = events.present_groups_stage(conn, stage_id)
    elif stage_kind == "round_robin":
        return HTMLResponse("<div class='error-banner'>Fase non trovata</div>")
    elif stage_kind == "single_elimination":
        stage = events.present_single_elimination_stage(conn, stage_id)

    stage["name"] = stage_label

    response = templates.TemplateResponse(
        request, "event_stage.html",
        {
            "stage": stage,
            "stage_kind": stage_kind,
            "stage_order": stage_order,
            "total_stages": total_stages,
            "event_id": event_id,
            "stage_id": stage_id
        }
    )

    return response


@app.post("/api/events/{event_id}/stages/{stage_id}/resize")
def resize_stage_groups(
    request: Request,
    event_id: int,
    stage_id: int,
    num_groups: int = Form(...),
):
    conn = request.state.conn

    # TODO: check that teh user is authorized to resize the stage group
    #       this check must always include a check on the version number in the events table
    #       if that has changed (for whatever reason!) the user must not change anything
    #       it must first reload the page so that he have the up-to-date content
    #       so a pop-up must ask the user to reload the page, this can be the usual catch all (at leat for now)
    #       then we will think to have one specific to reload the event page the user was working on
    #       If I want to use verify_event_access for this it means verify_event_access must also always check
    #       the version number of the event

    event_row = conn.execute(
        "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    if not event_row or event_row["current_stage_order"] != 0:
        return HTMLResponse("<div class='error-banner'>Resize consentito solo durante la registrazione</div>")

    stage_row = conn.execute(
        "SELECT id, stage_order, kind FROM event_stages WHERE id = ? AND event_id = ?",
        (stage_id, event_id)
    ).fetchone()
    if not stage_row or stage_row["kind"] not in ("groups", "round_robin"):
        return HTMLResponse("<div class='error-banner'>Fase non trovata o già iniziata</div>")

    total = conn.execute(
        "SELECT COUNT(*) as c FROM event_participants WHERE event_id = ?",
        (event_id,)
    ).fetchone()["c"]

    if total < 2:
        return HTMLResponse("<div class='error-banner'>Servono almeno 2 partecipanti</div>")

    events.generate_groups_stage(conn, stage_id, num_groups)
    conn.commit()

    # Re-render only the groups section
    stage = events.present_groups_stage(conn, stage_id)

    return templates.TemplateResponse(
        request, "stage_groups.html",
        {
            "stage": stage,
            "event_id": event_id
        }
    )


# ---------------------------------------------------------------------------
# Entity helpers - shared logic
# ---------------------------------------------------------------------------

def _list_entities(request: Request, entities: str):
    res, response = check_required_olympiad(request, entities)
    if not res:
        return response

    conn = request.state.conn
    olympiad_id = get_olympiad_from_request(request)["id"]
    items = conn.execute(
        f"SELECT e.id, e.name, e.version FROM {entities} e WHERE e.olympiad_id = ?",
        (olympiad_id,)
    ).fetchall()
    placeholder = entity_list_form_placeholder[entities]
    return templates.TemplateResponse(
        request,
        "entity_list.html",
        {
            "entities": entities,
            "placeholder": placeholder,
            "items": items
        }
    )


def _create_entity(request: Request, entities: str, name: str):
    olympiad_data = get_olympiad_from_request(request)
    olympiad_id   = olympiad_data["id"]

    if olympiad_id == 0:
        return templates.TemplateResponse(request, "select_olympiad_required.html")


    res, response = check_olympiad_auth(request, olympiad_id, olympiad_data["name"])
    if not res:
        return response

    conn = request.state.conn
    if entities == "events":
        insert_sql = f"""
            INSERT INTO {entities} (name, olympiad_id, score_kind)
            VALUES (?, ?, 'points')
            RETURNING id, name, version
            """
    else:
        insert_sql = f"""
            INSERT INTO {entities} (name, olympiad_id)
            VALUES (?, ?)
            RETURNING id, name, version
            """
    try:
        inserted_row = conn.execute(
            insert_sql, (name, olympiad_id)
        ).fetchone()

        if entities == "players":
            conn.execute(
                "INSERT INTO participants (player_id, team_id) VALUES (?, ?)",
                (inserted_row["id"], None)
            )
        else:
            conn.execute(
                "INSERT INTO participants (player_id, team_id) VALUES (?, ?)",
                (None, inserted_row["id"])
            )

        item = {"id": inserted_row["id"], "name": inserted_row["name"], "version": inserted_row["version"]}
        html_content = templates.get_template("entity_element.html").render(
            item=item, entities=entities, hx_target="#main-content"
        )
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = "#entity-list"
        response.headers["HX-Reswap"] = "afterbegin"
        conn.commit()
        return response
    except sqlite3.IntegrityError:
        conn.rollback()
        response = HTMLResponse(templates.get_template("entity_name_duplicate.html").render(entities=entities))
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response


def _rename_entity(
    request: Request,
    entities: str,
    entity_id: int,
    entity_curr_name: str,
    entity_new_name: str
):
    olympiad_data = get_olympiad_from_request(request)
    olympiad_id = olympiad_data["id"]
    olympiad_name = olympiad_data["name"]

    res, response = check_olympiad_auth(request, olympiad_id, olympiad_name)
    if not res: return response

    conn = request.state.conn
    try:
        updated_row = conn.execute(
            f"""
            UPDATE {entities}
            SET name = ?, version = version + 1
            WHERE id = ? AND name = ? AND olympiad_id = ?
            RETURNING id, name, version
            """,
            (entity_new_name, entity_id, entity_curr_name, olympiad_id)
        ).fetchone()
        if not updated_row:
            conn.rollback()
            # Entity was deleted or renamed by someone else
            entity = conn.execute(
                f"SELECT id, name, version FROM {entities} WHERE id = ?",
                (entity_id,)
            ).fetchone()
            if not entity:
                html_content = templates.get_template("entity_deleted_oob.html").render()
                response = HTMLResponse(html_content)
                response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
                response.headers["HX-Reswap"] = "outerHTML"
            else:
                item = {"id": entity_id, "name": entity["name"], "version": entity["version"]}
                html_content = templates.get_template("entity_renamed_oob.html").render(
                    item=item, entities=entities, hx_target="#main-content"
                )
                response = HTMLResponse(html_content)
                response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
                response.headers["HX-Reswap"] = "outerHTML"
            return response
        else:
            item = {"id": entity_id, "name": updated_row["name"], "version": updated_row["version"]}
            html_content = templates.get_template("entity_element.html").render(
                item=item, entities=entities, hx_target="#main-content"
            )
            response = HTMLResponse(html_content)
            response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
            response.headers["HX-Reswap"] = "outerHTML"
            conn.commit()
            return response
    except sqlite3.IntegrityError:
        conn.rollback()
        response = HTMLResponse(templates.get_template("entity_name_duplicate.html").render(entities=entities))
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response


def _delete_entity(request: Request, entities: str, entity_id: int, entity_name: str):
    olympiad_data = get_olympiad_from_request(request)
    olympiad_id = olympiad_data["id"]
    olympiad_name = olympiad_data["name"]

    res, response = check_olympiad_auth(request, olympiad_id, olympiad_name)
    if not res: return response

    conn = request.state.conn
    deleted_row = conn.execute(
        f"""
        DELETE FROM {entities}
        WHERE id = ? AND name = ? AND olympiad_id = ?
        RETURNING id
        """,
        (entity_id, entity_name, olympiad_id)
    ).fetchone()
    if not deleted_row:
        conn.rollback()
        # Entity was deleted or renamed by someone else
        entity = conn.execute(
            f"SELECT id, name, version FROM {entities} WHERE id = ?",
            (entity_id,)
        ).fetchone()
        if not entity:
            html_content = templates.get_template("entity_deleted_oob.html").render()
            response = HTMLResponse(html_content)
            response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
            response.headers["HX-Reswap"] = "outerHTML"
        else:
            item = {"id": entity_id, "name": entity["name"], "version": entity["version"]}
            html_content = templates.get_template("entity_renamed_oob.html").render(
                item=item, entities=entities, hx_target="#main-content"
            )
            response = HTMLResponse(html_content)
            response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
            response.headers["HX-Reswap"] = "outerHTML"
        return response
    else:
        html_content = templates.get_template("entity_delete.html").render()
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        conn.commit()
        return response


# ---------------------------------------------------------------------------
# Explicit entity routes - players, teams, events, edit/cancel-edit
# ---------------------------------------------------------------------------

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
    return _create_entity(request, "players", name)

@app.post("/api/teams")
def create_team(request: Request, name: str = Form(...)):
    return _create_entity(request, "teams", name)

@app.post("/api/events")
def create_event(request: Request, name: str = Form(...)):
    olympiad_data = get_olympiad_from_request(request)
    olympiad_id = olympiad_data["id"]
    olympiad_name = olympiad_data["name"]

    res, response = check_olympiad_auth(request, olympiad_id, olympiad_name)
    if not res:
        return response

    conn = request.state.conn
    try:
        row = conn.execute(
            "INSERT INTO events (name, olympiad_id, score_kind) VALUES (?, ?, 'points') RETURNING id, name, version",
            (name, olympiad_id)
        ).fetchone()
        response = templates.TemplateResponse(
            request,
            "event_page.html",
            {
                "event": {
                    "id": row["id"],
                    "name": row["name"],
                    "version": row["version"],
                    "status": "registration",
                },
            }
        )
        conn.commit()
        return response
    except sqlite3.IntegrityError:
        conn.rollback()
        response = HTMLResponse(templates.get_template("entity_name_duplicate.html").render(entities="events"))
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response


# ---------------------------------------------------------------------------
# Event live-editing endpoints
# ---------------------------------------------------------------------------

@app.put("/api/events/{event_id}/score_kind")
def update_event_score_kind(request: Request, event_id: int, score_kind: str = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    conn.execute("BEGIN IMMEDIATE")

    olympiad_id = verify_event_access(conn, session_id, event_id)

    if not olympiad_id:
        conn.rollback()
        return diagnose_event_noop(request, event_id, session_id)

    conn.execute("UPDATE events SET score_kind = ? WHERE id = ?", (score_kind, event_id))
    conn.commit()

    response = templates.TemplateResponse(
        request,
        "score_kind.html",
        {
            "event_id": event_id,
            "score_kinds": SCORE_KINDS,
            "current_score_kind": score_kind,
        }
    )
    response.headers["HX-Retarget"] = "#score-kind-section"
    response.headers["HX-Reswap"] = "outerHTML"
    return response


@app.get("/api/events/{event_id}/setup")
def get_event_setup(request: Request, event_id: int, version: int = Query(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    # TODO: even if it is just a user that is looking and not modifying
    #       he must receive a feedback if the name of the event has changed of the event does not exist
    event = conn.execute(
        "SELECT id FROM events WHERE id = ? AND version = ?", (event_id, version)
    ).fetchone()
    if not event:
        return diagnose_event_noop(request, event_id, session_id)

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

    return templates.TemplateResponse(
        request,
        "event_setup.html",
        {
            "event_id": event_id,
            "score_kinds": SCORE_KINDS,
            "current_score_kind": current_score_kind,
            "stage_kinds": stage_kinds,
            "stages": stages,
        }
    )


@app.post("/api/events/{event_id}/stages")
def add_event_stage(request: Request, event_id: int, kind: str = Form(...)):
    conn = request.state.conn

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

    conn.commit()

    return _render_stages_section(request, conn, event_id)


@app.delete("/api/events/{event_id}/stages/{stage_id}")
def remove_event_stage(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    conn.execute("DELETE FROM event_stages WHERE id = ? AND event_id = ?", (stage_id, event_id))

    # Re-sequence remaining stages
    remaining = conn.execute(
        "SELECT id FROM event_stages WHERE event_id = ? ORDER BY stage_order",
        (event_id,)
    ).fetchall()
    for i, row in enumerate(remaining):
        conn.execute(
            "UPDATE event_stages SET stage_order = ? WHERE id = ?",
            (i + 1, row["id"])
        )
    conn.commit()

    return _render_stages_section(request, conn, event_id)


def _render_stages_section(request, conn, event_id):
    """Re-render the stages setup section for the event page."""
    stage_kinds = conn.execute(
        "SELECT kind, label FROM stage_kinds ORDER BY kind"
    ).fetchall()

    stages = conn.execute(
        "SELECT es.id, es.stage_order, es.kind, sk.label "
        "FROM event_stages es JOIN stage_kinds sk ON sk.kind = es.kind "
        "WHERE es.event_id = ? ORDER BY es.stage_order",
        (event_id,)
    ).fetchall()

    return templates.TemplateResponse(
        request,
        "event_stages_setup.html",
        {
            "event_id": event_id,
            "stage_kinds": stage_kinds,
            "stages": stages,
        }
    )


@app.post("/api/events/{event_id}/enrollv2/{participant_id}")
def enrollv2_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    conn.execute(
        """
        INSERT OR IGNORE INTO event_participants
        (event_id, participant_id) VALUES (?, ?)
        """,
        (event_id, participant_id)
    )

    current_stage_order = conn.execute(
        "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
    ).fetchone()["current_stage_order"]
    assert current_stage_order == 0

    first_stage = conn.execute(
        """
        SELECT id, kind
        FROM event_stages WHERE event_id = ? AND stage_order = 1
        """,
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

    conn.commit()

    event_enrolled_participants = conn.execute(
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
    event_enrolled_ids = { p["id"] for p in event_enrolled_participants }

    olympiad_enrolled_participants = conn.execute(
        """
        SELECT p.id, COALESCE(pl.name, t.name) AS name
        FROM participants p
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        JOIN events e ON e.olympiad_id = COALESCE(pl.olympiad_id, t.olympiad_id)
        WHERE e.id = ?
        ORDER BY name
        """,
        (event_id,)
    ).fetchall()
    event_available_participants = [
        p for p in olympiad_enrolled_participants
        if p["id"] not in event_enrolled_ids
    ]

    return templates.TemplateResponse(
        request, "event_players_section_v2.html",
        {
            "enrolled_participants": event_enrolled_participants,
            "available_participants": event_available_participants,
            "event_id": event_id,
        }
    )


@app.delete("/api/events/{event_id}/enrollv2/{participant_id}")
def unenrollv2_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    # the pipeline must be
    #   delete the participants from the event: which means delete one row from event_participants
    #   regenerate the first stage 
    #   the problem right now is that I am generateing stuff for the second stage
    #   to solve this I can check event_id and allow unenroll only when current_stage_order is 0
    #   meaning the event has no started yet
    #   I must only put stuff on the first stage

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

    conn.commit()

    event_enrolled_participants = conn.execute(
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
    event_enrolled_ids = { p["id"] for p in event_enrolled_participants }

    olympiad_enrolled_participants = conn.execute(
        """
        SELECT p.id, COALESCE(pl.name, t.name) AS name
        FROM participants p
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        JOIN events e ON e.olympiad_id = COALESCE(pl.olympiad_id, t.olympiad_id)
        WHERE e.id = ?
        ORDER BY name
        """,
        (event_id,)
    ).fetchall()
    event_available_participants = [
        p for p in olympiad_enrolled_participants
        if p["id"] not in event_enrolled_ids
    ]

    response = templates.TemplateResponse(
        request, "event_players_section_v2.html",
        {
            "enrolled_participants": event_enrolled_participants,
            "available_participants": event_available_participants,
            "event_id": event_id,
        }
    )

    return response


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


@app.put("/api/players/{entity_id}")
def rename_players(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    return _rename_entity(request, "players", entity_id, curr_name, new_name)

@app.put("/api/teams/{entity_id}")
def rename_teams(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    return _rename_entity(request, "teams", entity_id, curr_name, new_name)

@app.put("/api/events/{entity_id}")
def rename_events(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    return _rename_entity(request, "events", entity_id, curr_name, new_name)


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
# Auth
# ---------------------------------------------------------------------------

@app.post("/api/validate_pin")
def validate_pin(request: Request, pin: str = Form(...), olympiad_id: int = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    # Try conditional insert - only succeeds if PIN is correct
    inserted = conn.execute(
        """
        INSERT INTO session_olympiad_auth (session_id, olympiad_id)
        SELECT ?, ?
        FROM olympiads
        WHERE id = ? AND pin = ?
        ON CONFLICT DO NOTHING
        RETURNING session_id
        """,
        (session_id, olympiad_id, olympiad_id, pin)
    ).fetchone()

    if inserted:
        conn.commit()
        response = HTMLResponse("")
        response.headers["HX-Trigger"] = "pinValidated"
        return response

    # No insert - diagnose why
    diag = conn.execute(
        """
        SELECT
          o.id IS NOT NULL as olympiad_exists,
          o.pin = ? as pin_correct,
          soa.session_id IS NOT NULL as already_authorized
        FROM (SELECT 1) dummy
        LEFT JOIN olympiads o ON o.id = ?
        LEFT JOIN session_olympiad_auth soa ON soa.session_id = ? AND soa.olympiad_id = ?
        """,
        (pin, olympiad_id, session_id, olympiad_id)
    ).fetchone()

    # Wrong PIN - show error modal
    if diag["olympiad_exists"] and not diag["pin_correct"]:
        template_ctx = {"olympiad_id": olympiad_id, "error": "PIN errato"}
        response = templates.TemplateResponse(request, "pin_modal.html", template_ctx)
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    # Olympiad deleted or already authorized - let the re-triggered request handle it
    response = HTMLResponse("")
    response.headers["HX-Trigger"] = "pinValidated"
    return response


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8080)
