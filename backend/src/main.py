import os
import json
import secrets
import asyncio
from collections import defaultdict

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import JSONResponse, HTMLResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from jinja2_fragments import render_block as _jinja2_render_block
from pathlib import Path

from . import database
from . import events

from enum import Enum

class Status(Enum):
    SUCCESS                    = "success"
    OLYMPIAD_NOT_FOUND         = "olympiad_not_found"
    OLYMPIAD_RENAMED           = "olympiad_renamed"
    OLYMPIAD_NOT_SELECTED      = "olympiad_not_selectec"
    NOT_AUTHORIZED             = "not_authorized"
    NAME_DUPLICATION           = "name_duplication"
    INVALID_PIN                = "invalid_pin"
    ENTITY_NOT_FOUND           = "entity_not_found"
    ENTITY_RENAMED             = "entity_renamed"
    EVENT_NOT_IN_REGISTRATION  = "event_not_in_registration"
    EVENT_IN_REGISTRATION      = "event_in_registration"
    EVENT_VERSION_OUTDATED     = "event_version_outdated"
    STAGE_INVALID              = "stage_invalid"
    NOT_ENOUGH_PARTICIPANTS    = "not_enough_participants"
    PLAYER_IN_RUNNING_EVENT    = "player_in_running_event"

db_path = Path(os.environ["DATABASE_PATH"])
schema_path = Path(os.environ["SCHEMA_PATH"])

root = Path(os.environ["PROJECT_ROOT"])
templates = Jinja2Templates(directory=root / "frontend" / "templates")

def render_event_fragment(block_name: str, **ctx) -> str:
    return _jinja2_render_block(templates.env, "event_page.html", block_name, **ctx)

def render_entity_fragment(block_name: str, **ctx) -> str:
    return _jinja2_render_block(templates.env, "entity_list.html", block_name, **ctx)

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


_event_subscribers: dict[int, set] = defaultdict(set)

def notify_event(event_id: int, event_name: str):
    msg = f"event: {event_name}\ndata: \n\n"
    for queue in list(_event_subscribers.get(event_id, [])):
        queue.put_nowait(msg)


def notify_olympiad_events(conn, olympiad_id: int, event_name: str):
    rows = conn.execute("SELECT id FROM events WHERE olympiad_id = ?", (olympiad_id,)).fetchall()
    for row in rows:
        notify_event(row["id"], event_name)


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


def _render_operation_denied(result, olympiad_id, entities):
    html_content = ""
    extra_headers = {}

    needs_modal = (
        result == Status.OLYMPIAD_NOT_FOUND          or
        result == Status.OLYMPIAD_RENAMED            or
        result == Status.NAME_DUPLICATION            or
        result == Status.NOT_AUTHORIZED              or
        result == Status.OLYMPIAD_NOT_SELECTED       or
        result == Status.EVENT_NOT_IN_REGISTRATION   or
        result == Status.EVENT_IN_REGISTRATION       or
        result == Status.EVENT_VERSION_OUTDATED      or
        result == Status.STAGE_INVALID               or
        result == Status.NOT_ENOUGH_PARTICIPANTS     or
        result == Status.PLAYER_IN_RUNNING_EVENT
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
    elif result in (
        Status.EVENT_NOT_IN_REGISTRATION,
        Status.EVENT_VERSION_OUTDATED,
        Status.STAGE_INVALID,
        Status.NOT_ENOUGH_PARTICIPANTS,
        Status.EVENT_IN_REGISTRATION,
        Status.PLAYER_IN_RUNNING_EVENT
    ):
        html_content = templates.get_template("operation_failed.html").render(result=result.value)

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


def check_event_in_registration(request: Request, event_id: int):
    row = request.state.conn.execute(
        "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    return row and row["current_stage_order"] == 0


def check_event_version(request: Request, event_id: int, version: int):
    row = request.state.conn.execute(
        "SELECT version FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    return row and row["version"] == version


def check_stage_kind_valid(request: Request, stage_id: int, event_id: int):
    row = request.state.conn.execute(
        "SELECT kind FROM event_stages WHERE id = ? AND event_id = ?", (stage_id, event_id)
    ).fetchone()
    return row and row["kind"] in ("groups", "round_robin")


def check_player_in_running_event(request: Request, player_id: int) -> bool:
    row = request.state.conn.execute(
        """
        SELECT 1 FROM event_participants ep
        JOIN participants p ON p.id = ep.participant_id
        JOIN events e ON e.id = ep.event_id
        WHERE p.player_id = ? AND e.current_stage_order > 0
        """,
        (player_id,)
    ).fetchone()
    return row is not None


def check_min_participants(request: Request, event_id: int, min_count: int):
    total = request.state.conn.execute(
        "SELECT COUNT(*) as c FROM event_participants WHERE event_id = ?", (event_id,)
    ).fetchone()["c"]
    return total >= min_count


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
        html_content = render_entity_fragment(
            "entity_list", entities="olympiads", placeholder=placeholder, items=rows
        )
    
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
        html_content = render_entity_fragment("entity_element", item=item, entities="olympiads", hx_target=f"#olympiads-{olympiad_id}")

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
            html_content = render_entity_fragment("entity_renamed_oob", entities="olympiads", item=olympiad_data, hx_target=hx_target)
            html_content += _oob_badge_html(request, olympiad_data["id"])
        else:
            html_content = render_entity_fragment("entity_element", entities="olympiads", item=olympiad_data, hx_target=hx_target)
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


@app.get("/api/events/{event_id}/matches/{match_id}/score/edit")
def get_edit_score(
    request: Request,
    event_id: int,
    match_id: int,
    p1_id: int = Query(...),
    p2_id: int = Query(...),
    score_kind: str = Query(...)
):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    result = Status.SUCCESS
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        score_rows = conn.execute(
            "SELECT participant_id, score FROM match_participant_scores WHERE match_id = ?",
            (match_id,)
        ).fetchall()
        score_map = { r["participant_id"]: r["score"] for r in score_rows }

        def get_participant_name(pid):
            row = conn.execute(
                "SELECT COALESCE(pl.name, t.name) AS name FROM participants p "
                "LEFT JOIN players pl ON pl.id = p.player_id "
                "LEFT JOIN teams t ON t.id = p.team_id WHERE p.id = ?", (pid,)
            ).fetchone()
            return row["name"] if row else str(pid)

        template_ctx = {
            "event_id": event_id,
            "match_id": match_id,
            "p1_id": p1_id,
            "p2_id": p2_id,
            "p1_name": get_participant_name(p1_id),
            "p2_name": get_participant_name(p2_id),
            "score_kind": score_kind,
            "p1_score": score_map.get(p1_id),
            "p2_score": score_map.get(p2_id),
        }
        html_content = templates.get_template("edit_score.html").render(**template_ctx)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


@app.get("/api/events/{event_id}/matches/{match_id}/score/cancel-edit")
def cancel_edit_score(
    request: Request,
    event_id: int,
    match_id: int,
    p1_id: int = Query(...),
    p2_id: int = Query(...),
    score_kind: str = Query(...)
):
    score_rows = request.state.conn.execute(
        "SELECT participant_id, score FROM match_participant_scores WHERE match_id = ?",
        (match_id,)
    ).fetchall()
    score_map = {r["participant_id"]: r["score"] for r in score_rows}
    p1_score = score_map.get(p1_id)
    p2_score = score_map.get(p2_id)
    score_str = (
        f"{p1_score} - {p2_score}"
        if p1_score is not None and p2_score is not None else None
    )
    ctx = {
        "event_id": event_id,
        "match_id": match_id,
        "p1_id": p1_id,
        "p2_id": p2_id,
        "score_kind": score_kind,
        "score": score_str,
    }
    html_content = templates.get_template("score_cell.html").render(**ctx)
    response = HTMLResponse(html_content)

    return response


@app.put("/api/events/{event_id}/matches/{match_id}/score")
async def update_match_score(
    request: Request,
    event_id: int,
    match_id: int,
    p1_id: int = Form(...),
    p2_id: int = Form(...),
    score_kind: str = Form(...),
    p1_score: int = Form(None),
    p2_score: int = Form(None),
    outcome: str = Form(None)
):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        if score_kind == "outcome":
            if outcome == "p1":
                p1_score, p2_score = 1, 0
            elif outcome == "p2":
                p1_score, p2_score = 0, 1
            else:
                p1_score, p2_score = 0, 0

        for pid, score in [(p1_id, p1_score), (p2_id, p2_score)]:
            conn.execute(
                "INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?) "
                "ON CONFLICT (match_id, participant_id) DO UPDATE SET score = excluded.score",
                (match_id, pid, score)
            )

        stage_row = conn.execute(
            "SELECT g.event_stage_id, es.kind FROM matches m "
            "JOIN groups g ON g.id = m.group_id "
            "JOIN event_stages es ON es.id = g.event_stage_id "
            "WHERE m.id = ?",
            (match_id,)
        ).fetchone()
        stage_id = stage_row["event_stage_id"]
        stage_kind = stage_row["kind"]

        if stage_kind == "single_elimination":
            winner_id = events.determine_bracket_winner(p1_id, p1_score, p2_id, p2_score, score_kind)
            events.advance_bracket_winner(conn, match_id, winner_id)

        new_event_version = conn.execute(
            "UPDATE events SET version = version + 1 WHERE id = ? RETURNING version",
            (event_id,)
        ).fetchone()["version"]

        notify_event(event_id, "score-update")

        if stage_kind == "single_elimination":
            stage = events.present_single_elimination_stage(conn, stage_id)
            html_content = render_event_fragment(
                "stage_bracket_inner",
                stage=stage,
                event_id=event_id,
                event_version=new_event_version
            )
            extra_headers["HX-Retarget"] = "#stage-bracket-inner"
        else:
            stage = events.present_groups_stage(conn, stage_id)
            html_content = render_event_fragment(
                "stage_groups_inner",
                stage=stage,
                event_id=event_id,
                event_version=new_event_version
            )
            extra_headers["HX-Retarget"] = "#stage-groups-inner"

        extra_headers["HX-Reswap"] = "outerHTML"

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.get("/api/events/{event_id}/stages/{stage_id}/bracket-content")
def get_bracket_content(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    stage = events.present_single_elimination_stage(conn, stage_id)
    event_version = conn.execute("SELECT version FROM events WHERE id = ?", (event_id,)).fetchone()["version"]
    return HTMLResponse(render_event_fragment("stage_bracket_inner", stage=stage, event_id=event_id, event_version=event_version))


def _cancel_edit(request: Request, entities: str, item_id: int, name: str):
    hx_target = "#olympiad-badge" if entities == "olympiads" else "#main-content"
    return HTMLResponse(render_entity_fragment("entity_element", item={"id": item_id, "name": name}, entities=entities, hx_target=hx_target))

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
        html_content = render_entity_fragment("entity_renamed_oob", entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)
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
        html_content = render_entity_fragment("entity_element", item=item, entities="olympiads", hx_target=hx_target)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)
    
    if result == Status.SUCCESS:
        conn.commit()
        event_ids = conn.execute(
            "SELECT id FROM events WHERE olympiad_id = ?", (olympiad_id,)
        ).fetchall()
        for row in event_ids:
            notify_event(row["id"], f"olympiad-renamed-{olympiad_id}")
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
        html_content = render_entity_fragment("entity_renamed_oob", entities="olympiads", item=olympiad_badge_ctx, hx_target=hx_target)
    elif result == Status.NOT_AUTHORIZED:
        extra_headers["HX-Pin-Required"] = "true"
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)
    else:
        event_ids = conn.execute(
            "SELECT id FROM events WHERE olympiad_id = ?", (olympiad_id,)
        ).fetchall()
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
        for row in event_ids:
            notify_event(row["id"], f"olympiad-deleted-{olympiad_id}")
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
        html_content = render_entity_fragment("entity_list", entities=entities, placeholder=placeholder, items=items)

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
    if not check_olympiad_exist(request, olympiad_id):
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
        html_content = render_entity_fragment("entity_element", item=item, entities="players", hx_target="#main-content")
        extra_headers["HX-Retarget"] = "#entity-list"
        extra_headers["HX-Reswap"] = "afterbegin"

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_olympiad_events(conn, olympiad_id, "enrollment-update")
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
    if not check_olympiad_exist(request, olympiad_id):
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
        html_content = render_entity_fragment("entity_element", item=item, entities="teams", hx_target="#main-content")
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
        html_content = render_event_fragment(
            "event_page",
            event_id=event["id"],
            event_name=event["name"],
            event_version=event["version"],
            event_status="registration",
            olympiad_id=olympiad_id,
            score_kinds=SCORE_KINDS,
            current_score_kind=SCORE_KINDS[0]["kind"]
        )

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
    if result == Status.SUCCESS and entities == "players" and check_player_in_running_event(request, entity_id):
        result = Status.PLAYER_IN_RUNNING_EVENT

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, entities)

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.ENTITY_RENAMED:
        entity = conn.execute(f"SELECT * FROM {entities} WHERE id = ?", (entity_id,)).fetchone()
        entity_data = { "id": entity_id, "name": entity["name"], "version": entity["version"] }
        html_content = render_entity_fragment("entity_renamed_oob", entities=entities, item=entity_data, hx_target=hx_target)
    elif result == Status.SUCCESS:
        updated_row = conn.execute(
            f"UPDATE {entities} SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (entity_new_name, entity_id)
        ).fetchone()
        item = {"id": entity_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = render_entity_fragment("entity_element", item=item, entities=entities, hx_target=hx_target)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)
    
    if result == Status.SUCCESS:
        conn.commit()
        if entities == "events":
            notify_event(entity_id, "event-renamed")
        elif entities == "players":
            notify_olympiad_events(conn, olympiad_id, "enrollment-update")
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
    if result == Status.SUCCESS and entities == "players" and check_player_in_running_event(request, entity_id):
        result = Status.PLAYER_IN_RUNNING_EVENT

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, entities)

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
    elif result == Status.ENTITY_RENAMED:
        entity = conn.execute(f"SELECT * FROM {entities} WHERE id = ?", (entity_id,)).fetchone()
        entity_data = { "id": entity_id, "name": entity["name"], "version": entity["version"] }
        html_content = render_entity_fragment("entity_renamed_oob", entities=entities, item=entity_data, hx_target=hx_target)
    elif result == Status.SUCCESS:
        conn.execute(f"DELETE FROM {entities} WHERE id = ?", (entity_id,))
        html_content = templates.get_template("entity_delete.html").render()

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        if entities == "events":
            notify_event(entity_id, "event-deleted")
            for queue in list(_event_subscribers.get(entity_id, [])):
                queue.put_nowait(None)
            _event_subscribers.pop(entity_id, None)
        elif entities == "players":
            notify_olympiad_events(conn, olympiad_id, "enrollment-update")
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
def select_event(request: Request, event_id: int, event_name: str = Query(None, alias="name")):
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
    if result == Status.SUCCESS and event_name is not None and not check_entity_name(request, "events", event_id, event_name):
        result = Status.ENTITY_RENAMED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_deleted_oob.html").render()
        extra_headers["HX-Retarget"] = f"#events-{event_id}"
        extra_headers["HX-Reswap"] = "outerHTML"
    elif result == Status.ENTITY_RENAMED:
        event = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        event_data = {"id": event_id, "name": event["name"], "version": event["version"]}
        html_content = render_entity_fragment("entity_renamed_oob", entities="events", item=event_data, hx_target=hx_target)
        extra_headers["HX-Retarget"] = f"#events-{event_id}"
        extra_headers["HX-Reswap"] = "outerHTML"
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
        extra_ctx = {}
        if event_status == "registration":
            extra_ctx = _get_registration_ctx(conn, event_id, olympiad_id, event["score_kind"])
        else:
            extra_ctx = _get_stage_ctx(conn, event_id, event["current_stage_order"])

        html_content = render_event_fragment(
            "event_page",
            event_id=event["id"],
            event_name=event["name"],
            event_version=event["version"],
            event_status=event_status,
            olympiad_id=olympiad_id, **extra_ctx
        )

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


def _get_registration_ctx(conn, event_id: int, olympiad_id: int, score_kind: str) -> dict:
    stage_kinds = conn.execute(
        "SELECT kind, label FROM stage_kinds ORDER BY kind"
    ).fetchall()
    stages = conn.execute(
        "SELECT es.id, es.stage_order, es.kind, sk.label, "
        "COALESCE((SELECT COUNT(*) FROM groups g WHERE g.event_stage_id = es.id), 0) AS num_groups "
        "FROM event_stages es JOIN stage_kinds sk ON sk.kind = es.kind "
        "WHERE es.event_id = ? ORDER BY es.stage_order",
        (event_id,)
    ).fetchall()
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
    return dict(
        score_kinds=SCORE_KINDS,
        current_score_kind=score_kind,
        stage_kinds=stage_kinds,
        stages=stages,
        enrolled_participants=enrolled_participants,
        available_participants=available_participants,
    )


def _get_stage_ctx(conn, event_id: int, stage_order: int) -> dict:
    row = conn.execute(
        """
        SELECT es.id, es.stage_order, es.kind, sk.label
        FROM event_stages es
        JOIN stage_kinds sk ON sk.kind = es.kind
        WHERE es.event_id = ? AND es.stage_order = ?
        """,
        (event_id, stage_order)
    ).fetchone()

    if not row:
        return {}

    stage_id   = row["id"]
    stage_kind = row["kind"]
    stage_label = row["label"]

    total_stages = conn.execute(
        "SELECT COUNT(*) AS count FROM event_stages WHERE event_id = ?",
        (event_id,)
    ).fetchone()["count"]

    if stage_kind == "groups":
        stage = events.present_groups_stage(conn, stage_id)
    elif stage_kind == "single_elimination":
        stage = events.present_single_elimination_stage(conn, stage_id)
    else:
        return {}

    stage["name"] = stage_label
    return dict(stage=stage, stage_kind=stage_kind, stage_order=stage_order, total_stages=total_stages)


def _set_event_stage_order(request: Request, event_id: int, new_stage_order: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
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
            "SELECT id, name, version, score_kind FROM events WHERE id = ?",
            (event_id,)
        ).fetchone()
        event_status = derive_event_status(new_stage_order, max_stage_order)
        extra_ctx = {}
        if event_status == "registration":
            extra_ctx = _get_registration_ctx(conn, event_id, olympiad_id, event["score_kind"])
        else:
            extra_ctx = _get_stage_ctx(conn, event_id, new_stage_order)

        html_content = render_event_fragment(
            "event_page",
            event_id=event["id"],
            event_name=event["name"],
            event_version=event["version"],
            event_status=event_status,
            olympiad_id=olympiad_id,
            **extra_ctx
        )

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "status-update")
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
    html_content = render_event_fragment(
        "event_player_container",
        event_id=event_id,
        enrolled_participants=enrolled_participants,
        available_participants=available_participants
    )

    response = HTMLResponse(html_content)

    return response


@app.get("/api/events/{event_id}/stage/{stage_order}")
def get_event_stage(request: Request, event_id: int, stage_order: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    row = conn.execute(
        """
        SELECT e.current_stage_order, e.version AS event_version, es.id, es.stage_order, es.kind, sk.label
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
            html_content = render_event_fragment(
                "stage_content",
                stage=stage,
                stage_kind=stage_kind,
                stage_order=stage_order,
                total_stages=total_stages,
                event_id=event_id, event_version=row["event_version"],
            )

    response = HTMLResponse(html_content)

    return response


@app.post("/api/events/{event_id}/stages/{stage_id}/resize")
def resize_stage_groups(
    request: Request,
    event_id: int,
    stage_id: int,
    num_groups: int = Form(...),
):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    # if not check_olympiad_exist(request, olympiad_id):
    #     result = Status.OLYMPIAD_NOT_FOUND
    # if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
    #     result = Status.OLYMPIAD_RENAMED
    # if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
    #     result = Status.ENTITY_NOT_FOUND
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED
    if result == Status.SUCCESS and not check_event_in_registration(request, event_id):
        result = Status.EVENT_NOT_IN_REGISTRATION
    if result == Status.SUCCESS and not check_stage_kind_valid(request, stage_id, event_id):
        result = Status.STAGE_INVALID
    if result == Status.SUCCESS and not check_min_participants(request, event_id, 2):
        result = Status.NOT_ENOUGH_PARTICIPANTS

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.ENTITY_NOT_FOUND:
        html_content = templates.get_template("entity_not_found.html").render()
    elif result == Status.SUCCESS:
        events.generate_groups_stage(conn, stage_id, num_groups)
        new_version = conn.execute(
            "UPDATE events SET version = version + 1 WHERE id = ? RETURNING version",
            (event_id,)
        ).fetchone()["version"]
        stage = events.present_groups_stage(conn, stage_id)
        html_content = render_event_fragment("stage_groups_content",
            stage=stage, event_id=event_id, event_version=new_version)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@app.get("/api/events/{event_id}/stages/{stage_id}/kind/edit")
def get_edit_stage_kind(request: Request, event_id: int, stage_id: int,):
    conn = request.state.conn
    row = conn.execute(
        """
        SELECT es.kind, sk.label
        FROM event_stages es
        JOIN stage_kinds sk ON sk.kind = es.kind
        WHERE es.id = ? AND es.event_id = ?
        """,
        (stage_id, event_id)
    ).fetchone()
    stage_kinds = conn.execute("SELECT kind, label FROM stage_kinds ORDER BY kind").fetchall()
    return templates.TemplateResponse(
        request,
        "edit_stage_kind.html",
        {
            "stage_id": stage_id,
            "event_id": event_id,
            "current_kind": row["kind"],
            "stage_kinds": stage_kinds,
        }
    )


@app.get("/api/events/{event_id}/stages/{stage_id}/kind/cancel-edit")
def cancel_edit_stage_kind(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    row = conn.execute(
        """
        SELECT es.kind, sk.label
        FROM event_stages es
        JOIN stage_kinds sk
        ON sk.kind = es.kind
        WHERE es.id = ? AND es.event_id = ?
        """,
        (stage_id, event_id)
    ).fetchone()
    return templates.TemplateResponse(
        request,
        "stage_kind_display.html",
        {
            "stage_id": stage_id,
            "event_id": event_id,
            "current_label": row["label"],
        }
    )


@app.patch("/api/events/{event_id}/stages/{stage_id}")
def update_stage_kind(request: Request, event_id: int, stage_id: int, kind: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        stage_order = conn.execute(
            "SELECT stage_order FROM event_stages WHERE id = ? AND event_id = ?",
            (stage_id, event_id)
        ).fetchone()["stage_order"]

        conn.execute("DELETE FROM event_stages WHERE id = ?", (stage_id,))

        new_id = conn.execute(
            "INSERT INTO event_stages (event_id, kind, stage_order) VALUES (?, ?, ?) RETURNING id",
            (event_id, kind, stage_order)
        ).fetchone()["id"]

        if stage_order == 1:
            if kind == "groups":
                events.generate_groups_stage(conn, new_id, 1)
            elif kind == "single_elimination":
                events.generate_single_elimination_stage(conn, new_id)

        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@app.post("/api/events/{event_id}/stages/{stage_id}/num-groups")
def set_stage_num_groups(request: Request, event_id: int, stage_id: int, num_groups: int = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        events.generate_groups_stage(conn, stage_id, max(1, num_groups))
        html_content = _render_stages_section_html(conn, event_id)

    html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Stage groups content (used by SSE-triggered refresh)
# ---------------------------------------------------------------------------

@app.get("/api/events/{event_id}/stages/{stage_id}/groups-content")
def get_stage_groups_content(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    stage = events.present_groups_stage(conn, stage_id)
    event_version = conn.execute(
        "SELECT version FROM events WHERE id = ?", (event_id,)
    ).fetchone()["version"]
    return HTMLResponse(render_event_fragment("stage_groups_inner",
        stage=stage, event_id=event_id, event_version=event_version))


# ---------------------------------------------------------------------------
# Event setup section refresh endpoints (used by SSE-triggered refresh)
# ---------------------------------------------------------------------------

@app.get("/api/events/{event_id}/score-kind-section")
def get_score_kind_section(request: Request, event_id: int):
    conn = request.state.conn
    row = conn.execute(
        "SELECT score_kind, version FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    return HTMLResponse(render_event_fragment("score_kind_section",
        event_id=event_id, event_version=row["version"],
        score_kinds=SCORE_KINDS,
        current_score_kind=row["score_kind"],
    ))


@app.get("/api/events/{event_id}/stages-section")
def get_stages_section(request: Request, event_id: int):
    conn = request.state.conn
    return HTMLResponse(_render_stages_section_html(conn, event_id))


@app.get("/api/events/{event_id}/deleted-notice")
def get_event_deleted_notice(request: Request, event_id: int):
    return templates.TemplateResponse(request, "event_deleted_modal.html", {})


@app.get("/api/events/{event_id}/olympiad-deleted-notice")
def get_event_olympiad_deleted_notice(request: Request, event_id: int):
    html_content = templates.get_template("olympiad_deleted_modal.html").render()
    html_content += templates.get_template("olympiad_badge.html").render(olympiad=sentinel_olympiad_badge, oob=True)
    return HTMLResponse(html_content)


@app.get("/api/events/{event_id}/olympiad-renamed-notice")
def get_event_olympiad_renamed_notice(request: Request, event_id: int):
    olympiad_data = request.state.conn.execute(
        "SELECT * FROM olympiads JOIN events ON olympiads.id = events.olympiad_id WHERE events.id = ?",
        (event_id,)
    ).fetchone()
    olympiad_id = olympiad_data["id"]
    olympiad_name = olympiad_data["name"]
    olympiad_version = olympiad_data["version"]

    html_content = templates.get_template("olympiad_renamed_modal.html").render()
    olympiad = {"id": olympiad_id, "name": olympiad_name, "version": olympiad_version}
    html_content += templates.get_template("olympiad_badge.html").render(olympiad=olympiad, oob=True)
    return HTMLResponse(html_content)


@app.get("/api/events/{event_id}/title")
def get_event_title(request: Request, event_id: int):
    event = request.state.conn.execute("SELECT name FROM events WHERE id = ?", (event_id,)).fetchone()
    return HTMLResponse(templates.get_template("event_title.html").render(name=event["name"]))


@app.get("/api/events/{event_id}/sse")
async def event_sse(request: Request, event_id: int):
    conn = request.state.conn
    # if not conn.execute("SELECT 1 FROM events WHERE id = ?", (event_id,)).fetchone():
    #     return Response(status_code=404)

    queue: asyncio.Queue = asyncio.Queue()
    _event_subscribers[event_id].add(queue)

    async def generate():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25.0)
                    if data is None:
                        break
                    yield data
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _event_subscribers[event_id].discard(queue)

    media_type = "text/event-stream"
    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(generate(), media_type=media_type, headers=headers)


# ---------------------------------------------------------------------------
# Event live-editing endpoints
# ---------------------------------------------------------------------------

@app.put("/api/events/{event_id}/score_kind")
def update_event_score_kind(request: Request, event_id: int, score_kind: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    # olympiad_name = olympiad_badge_ctx["name"]

    # assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    # if not check_olympiad_exist(request, olympiad_id):
    #     result = Status.OLYMPIAD_NOT_FOUND
    # if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
    #     result = Status.OLYMPIAD_RENAMED
    # if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
    #     result = Status.ENTITY_NOT_FOUND
    # if result == Status.SUCCESS and not check_event_in_registration(request, event_id):
    #     result = Status.EVENT_NOT_IN_REGISTRATION
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        event_version = conn.execute(
            "UPDATE events SET score_kind = ? WHERE id = ? RETURNING version",
            (score_kind, event_id)
        ).fetchone()["version"]
        html_content = render_event_fragment("score_kind_section",
            event_id=event_id, event_version=event_version,
            score_kinds=SCORE_KINDS,
            current_score_kind=score_kind,
        )
        extra_headers["HX-Retarget"] = "#score-kind-section"
        extra_headers["HX-Reswap"] = "outerHTML"

    # html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "score-kind-update")
    else:
        conn.rollback()

    return response


@app.get("/api/events/{event_id}/setup")
def get_event_setup(request: Request, event_id: int, version: int = Query(...)):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    # olympiad_name = olympiad_badge_ctx["name"]

    assert olympiad_id != 0

    result = Status.SUCCESS
    # if not check_olympiad_exist(request, olympiad_id):
    #     result = Status.OLYMPIAD_NOT_FOUND
    # if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
    #     result = Status.OLYMPIAD_RENAMED
    # if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
    #     result = Status.ENTITY_NOT_FOUND

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        current_score_kind = conn.execute(
            "SELECT score_kind FROM events WHERE id = ?", (event_id,)
        ).fetchone()["score_kind"]

        stage_kinds = conn.execute(
            "SELECT kind, label FROM stage_kinds ORDER BY kind"
        ).fetchall()

        stages = conn.execute(
            "SELECT es.id, es.stage_order, es.kind, sk.label, "
            "COALESCE((SELECT COUNT(*) FROM groups g WHERE g.event_stage_id = es.id), 0) AS num_groups "
            "FROM event_stages es JOIN stage_kinds sk ON sk.kind = es.kind "
            "WHERE es.event_id = ? ORDER BY es.stage_order",
            (event_id,)
        ).fetchall()

        html_content = render_event_fragment(
            "event_setup",
            event_id=event_id,
            event_version=version,
            score_kinds=SCORE_KINDS,
            current_score_kind=current_score_kind,
            stage_kinds=stage_kinds,
            stages=stages,
        )

    # html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


@app.post("/api/events/{event_id}/stages")
def add_event_stage(request: Request, event_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    stage_kinds = conn.execute("SELECT * FROM stage_kinds").fetchall()

    result = Status.SUCCESS
    if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        stage_kind = stage_kinds[0]["kind"]

        max_order = conn.execute(
            "SELECT COALESCE(MAX(stage_order), 0) AS m FROM event_stages WHERE event_id = ?",
            (event_id,)
        ).fetchone()["m"]

        stage_order = max_order + 1
        stage_id = conn.execute(
            "INSERT INTO event_stages (event_id, kind, stage_order) VALUES (?, ?, ?) RETURNING id",
            (event_id, stage_kind, stage_order)
        ).fetchone()["id"]

        if stage_order == 1:
            if stage_kind == "groups":
                events.generate_groups_stage(conn, stage_id, 1)
            elif stage_kind == "single_elimination":
                events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@app.delete("/api/events/{event_id}/stages/{stage_id}")
def remove_event_stage(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
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

        conn.execute("UPDATE events SET version = version + 1 WHERE id = ?", (event_id,))

        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


def _render_stages_section_html(conn, event_id: int):
    """Re-render the stages setup section for the event page."""

    stage_kinds = conn.execute("SELECT kind, label FROM stage_kinds ORDER BY kind").fetchall()

    stages = conn.execute(
        """
        SELECT es.id, es.stage_order, es.kind, sk.label,
               COALESCE((SELECT COUNT(*) FROM groups g WHERE g.event_stage_id = es.id), 0) AS num_groups
        FROM event_stages es
        JOIN stage_kinds sk
        ON sk.kind = es.kind
        WHERE es.event_id = ?
        ORDER BY es.stage_order
        """,
        (event_id,)
    ).fetchall()

    return render_event_fragment("stages_setup_section", event_id=event_id, stage_kinds=stage_kinds, stages=stages)


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

    return render_event_fragment(
        "event_player_container",
        event_id=event_id,
        enrolled_participants=enrolled_participants,
        available_participants=available_participants,
    )


@app.post("/api/events/{event_id}/enroll/{participant_id}")
def enroll_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    # olympiad_name = olympiad_badge_ctx["name"]

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    # if not check_olympiad_exist(request, olympiad_id):
    #     result = Status.OLYMPIAD_NOT_FOUND
    # if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
    #     result = Status.OLYMPIAD_RENAMED
    # if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
    #     result = Status.ENTITY_NOT_FOUND
    # if result == Status.SUCCESS and not check_user_authorized(request, olympiad_id):
    #     result = Status.NOT_AUTHORIZED
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
        conn.execute(
            "INSERT INTO event_participants (event_id, participant_id) VALUES (?, ?)",
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
                groups = conn.execute(
                    "SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)
                ).fetchall()
                events.generate_groups_stage(conn, stage_id, len(groups))
            elif stage_kind == "single_elimination":
                events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_event_players_section_html(conn, event_id, olympiad_id)

    # html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "enrollment-update")
    else:
        conn.rollback()

    return response


@app.delete("/api/events/{event_id}/enroll/{participant_id}")
def unenroll_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    # olympiad_name = olympiad_badge_ctx["name"]

    # assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = Status.SUCCESS
    # if not check_olympiad_exist(request, olympiad_id):
    #     result = Status.OLYMPIAD_NOT_FOUND
    # if result == Status.SUCCESS and not check_olympiad_name(request, olympiad_id, olympiad_name):
    #     result = Status.OLYMPIAD_RENAMED
    # if result == Status.SUCCESS and not check_entity_exist(request, "events", event_id):
    #     result = Status.ENTITY_NOT_FOUND
    if not check_user_authorized(request, olympiad_id):
        result = Status.NOT_AUTHORIZED

    html_content, extra_headers = _render_operation_denied(result, olympiad_id, "events")

    if result == Status.SUCCESS:
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
                groups = conn.execute(
                    "SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)
                ).fetchall()
                events.generate_groups_stage(conn, stage_id, len(groups))
            elif stage_kind == "single_elimination":
                events.generate_single_elimination_stage(conn, stage_id)

        html_content = _render_event_players_section_html(conn, event_id, olympiad_id)

    # html_content += _oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == Status.SUCCESS:
        conn.commit()
        notify_event(event_id, "enrollment-update")
    else:
        conn.rollback()

    return response

@app.post("/api/events/{event_id}/stage/{}")

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
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000, timeout_graceful_shutdown=1)
