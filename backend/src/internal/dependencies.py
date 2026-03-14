import os
from collections import defaultdict

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from jinja2_fragments import render_block as _jinja2_render_block
from pathlib import Path

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
    PREVIOUS_STAGE_INCOMPLETE  = "previous_stage_incomplete"


db_path = Path(os.environ["DATABASE_PATH"])
schema_path = Path(os.environ["SCHEMA_PATH"])
root = Path(os.environ["PROJECT_ROOT"])

templates = Jinja2Templates(directory=root / "frontend" / "templates")
root_templates = Jinja2Templates(directory=root / "frontend")


def render_event_fragment(block_name: str, **ctx) -> str:
    return _jinja2_render_block(templates.env, "event_page.html", block_name, **ctx)


def render_entity_fragment(block_name: str, **ctx) -> str:
    return _jinja2_render_block(templates.env, "entity_list.html", block_name, **ctx)


SCORE_KINDS = [
    {"kind": "points", "label": "Punti"},
    {"kind": "outcome", "label": "Vittoria / Sconfitta"},
]

STAGE_KIND_MAP = {
    ("pool",    2): {"kind": "groups",             "label": "Girone",                "advancement_mechanism": "pool",    "match_size": 2},
    ("pool",    0): {"kind": "individual_score",   "label": "Punteggio Individuale", "advancement_mechanism": "pool",    "match_size": 0},
    ("bracket", 2): {"kind": "single_elimination", "label": "Eliminazione Diretta",  "advancement_mechanism": "bracket", "match_size": 2},
}

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
_olympiad_subscribers: dict[int, set] = defaultdict(set)


def notify_event(event_id: int, event_name: str):
    msg = f"event: {event_name}\ndata: \n\n"
    for queue in list(_event_subscribers.get(event_id, [])):
        queue.put_nowait(msg)


def notify_olympiad(olympiad_id: int, event_name: str, exclude_tab_id: str = None):
    msg = f"event: {event_name}\ndata: \n\n"
    for tab_id, queue in list(_olympiad_subscribers.get(olympiad_id, [])):
        if tab_id != exclude_tab_id:
            queue.put_nowait(msg)


def notify_olympiad_events(conn, olympiad_id: int, event_name: str):
    rows = conn.execute("SELECT id FROM events WHERE olympiad_id = ?", (olympiad_id,)).fetchall()
    for row in rows:
        notify_event(row["id"], event_name)


############################### Database Queries ################################

def query_get_event_enrolled_participants(conn, event_id: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT ep.participant_id AS id, COALESCE(pl.name, t.name) AS name
        FROM event_participants ep
        JOIN participants p ON p.id = ep.participant_id
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        WHERE ep.event_id = ?
        """,
        (event_id,)
    ).fetchall()
    event_enrolled_participants = [{"id": row["id"], "name": row["name"]} for row in rows]
    return event_enrolled_participants


def query_get_olympiad_enrolled_participants(conn, olympiad_id: int) -> list[dict]:
    rows = conn.execute(
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
    olympiad_enrolled_participants = [{"id": row["id"], "name": row["name"]} for row in rows]
    return olympiad_enrolled_participants


def query_get_event_stages_with_num_groups(conn, event_id: int):
    rows = conn.execute(
        """
        SELECT es.id, es.stage_order, es.advancement_mechanism, es.match_size, es.advance_count,
        (SELECT COUNT(*) FROM groups g WHERE g.event_stage_id = es.id) AS num_groups
        FROM event_stages es
        WHERE es.event_id = ?
        ORDER BY es.stage_order
        """,
        (event_id,)
    ).fetchall()
    stages = [dict(row) for row in rows]
    return stages


def query_get_event_max_stage_order(conn, event_id: int):
    row = conn.execute(
        "SELECT MAX(stage_order) AS max_order FROM event_stages WHERE event_id = ?",
        (event_id,)
    ).fetchone()
    return row["max_order"] or 0


def query_get_stage_id_from_match_id(conn, match_id: int):
    stage_row = conn.execute(
        """
        SELECT g.event_stage_id
        FROM matches m
        JOIN groups g ON g.id = m.group_id
        WHERE m.id = ?
        """,
        (match_id,)
    ).fetchone()
    return stage_row["event_stage_id"]


def query_update_score(conn, match_id: int, participant_id: int, score: int):
    conn.execute(
        """
        INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?)
        ON CONFLICT (match_id, participant_id) DO UPDATE SET score = excluded.score
        """,
        (match_id, participant_id, score)
    )


def query_get_participant_name(conn, participant_id: int):
    row = conn.execute(
        """
        SELECT COALESCE(pl.name, t.name) AS name FROM participants p
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id WHERE p.id = ?
        """,
        (participant_id,)
    ).fetchone()
    return row["name"]


def query_get_score(conn, match_id: int, participant_id: int):
    score_row = conn.execute(
        "SELECT score FROM match_participant_scores WHERE match_id = ? AND participant_id = ?",
        (match_id, participant_id)
    ).fetchone()
    return score_row["score"] if score_row else 0


################################################################################

def derive_event_status(current_stage_order: int, max_stage_order: int):
    if current_stage_order == 0:
        event_status = "registration"
    elif current_stage_order <= max_stage_order:
        event_status = "started"
    else:
        event_status = "finished"
    return event_status


def get_olympiad_from_request(request: Request) -> dict:
    result = {
        "id": int(request.headers.get("X-Olympiad-Id", "0")),
        "version": int(request.headers.get("X-Olympiad-Version", "0")),
        "name": request.headers.get("X-Olympiad-Name", ""),
    }
    return result


def _oob_sse_link_html(olympiad_id: int, tab_id: str) -> str:
    html_content = templates.get_template("olympiad_sse_link.html")
    html_content = html_content.render(olympiad_id=olympiad_id, tab_id=tab_id)
    return html_content


def _oob_badge_html(request, olympiad_id: int):
    conn = request.state.conn
    tab_id = request.headers.get("X-Tab-Id", "")

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_badge_ctx["id"],)).fetchone()

    if olympiad_id == olympiad_badge_ctx["id"]:
        if not olympiad:
            return templates.get_template("olympiad_badge.html").render(olympiad=sentinel_olympiad_badge, tab_id=tab_id, oob=True)
        else:
            olympiad_data = {"id": olympiad["id"], "name": olympiad["name"], "version": olympiad["version"]}
            return templates.get_template("olympiad_badge.html").render(olympiad=olympiad_data, tab_id=tab_id, oob=True)
    else:
        return templates.get_template("olympiad_badge.html").render(olympiad=olympiad_badge_ctx, tab_id=tab_id, oob=True)


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
        result == Status.PLAYER_IN_RUNNING_EVENT     or
        result == Status.PREVIOUS_STAGE_INCOMPLETE
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
    elif result == Status.PREVIOUS_STAGE_INCOMPLETE:
        html_content = templates.get_template("previous_stage_incomplete.html").render()
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


def check_previous_stage_complete(request: Request, match_id: int):
    conn = request.state.conn

    stage_row = conn.execute(
        "SELECT es.stage_order, es.event_id "
        "FROM matches m "
        "JOIN groups g ON g.id = m.group_id "
        "JOIN event_stages es ON es.id = g.event_stage_id "
        "WHERE m.id = ?",
        (match_id,)
    ).fetchone()

    if stage_row["stage_order"] <= 1:
        return True

    incomplete_count = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM matches m
        JOIN groups g ON g.id = m.group_id
        JOIN event_stages es ON es.id = g.event_stage_id
        WHERE es.event_id = ? AND es.stage_order = ?
            AND (SELECT COUNT(*) FROM match_participants mp WHERE mp.match_id = m.id) >= 2
            AND (
                SELECT COUNT(*) FROM match_participants mp
                LEFT JOIN match_participant_scores mps
                ON mps.match_id = mp.match_id AND mps.participant_id = mp.participant_id
                WHERE mp.match_id = m.id AND mps.match_id IS NULL
            ) > 0
        """,
        (stage_row["event_id"], stage_row["stage_order"] - 1)
    ).fetchone()["cnt"]
    return incomplete_count == 0


def check_olympiad_name_duplication(request: Request, olympiad_id: int, name: str):
    result = request.state.conn.execute(
        """
        SELECT 1 FROM olympiads WHERE id != ? AND name = ?
        """,
        (olympiad_id, name)
    ).fetchone()
    return result


def check_entity_name_duplication(request: Request, olympiad_id: int, entities: str, entity_id: int, entity_name: str):
    result = request.state.conn.execute(
        f"""
        SELECT 1 FROM {entities}
        WHERE olympiad_id = ? AND id != ? AND name = ?
        """,
        (olympiad_id, entity_id, entity_name)
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
        "SELECT advancement_mechanism FROM event_stages WHERE id = ? AND event_id = ?", (stage_id, event_id)
    ).fetchone()
    return row and row["advancement_mechanism"] == "pool"


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


def _get_edit_textbox(request: Request, entities: str, item_id: int, name: str):
    template_ctx = {"curr_name": name, "entities": entities, "id": item_id}
    return templates.TemplateResponse(request, "edit_entity.html", template_ctx)


def _cancel_edit(request: Request, entities: str, item_id: int, name: str):
    hx_target = "#olympiad-badge" if entities == "olympiads" else "#main-content"
    return HTMLResponse(render_entity_fragment("entity_element", item={"id": item_id, "name": name}, entities=entities, hx_target=hx_target))


def _list_entities(request: Request, entities: str):
    conn = request.state.conn

    olympiad_badge_ctx = get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    result = Status.SUCCESS
    if olympiad_id == 0:
        result = Status.OLYMPIAD_NOT_SELECTED

    if result == Status.OLYMPIAD_NOT_SELECTED:
        html_content = templates.get_template("select_olympiad_required.html").render()

    if result == Status.SUCCESS:
        items = conn.execute(
            f"SELECT e.id, e.name, e.version FROM {entities} e WHERE e.olympiad_id = ?",
            (olympiad_id,)
        ).fetchall()
        placeholder = entity_list_form_placeholder[entities]
        html_content = render_entity_fragment("entity_list", entities=entities, placeholder=placeholder, items=items)

    response = HTMLResponse(html_content)

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
    if result == Status.SUCCESS and check_entity_name_duplication(request, olympiad_id, entities, 0, entity_new_name):
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
        entity_data = {"id": entity_id, "name": entity["name"], "version": entity["version"]}
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
        entity_data = {"id": entity_id, "name": entity["name"], "version": entity["version"]}
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
            _event_subscribers.pop(entity_id, None)
        elif entities == "players":
            notify_olympiad_events(conn, olympiad_id, "enrollment-update")
    else:
        conn.rollback()

    return response
