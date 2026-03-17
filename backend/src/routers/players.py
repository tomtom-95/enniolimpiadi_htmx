from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse

from ..internal import dependencies as dep

router = APIRouter(prefix="/api/players")


@router.get("")
def list_players(request: Request):
    conn = request.state.conn
    olympiad_id = dep.get_olympiad_from_request(request)["id"]
    items = conn.execute(
        "SELECT id, name, version FROM players WHERE olympiad_id = ?", (olympiad_id,)
    ).fetchall()
    html_content = dep.render_entity_fragment(
        "entity_list", entities="players", placeholder="Aggiungi un nuovo giocatore", items=items
    )
    return HTMLResponse(html_content)


@router.get("/{item_id}/edit")
def get_edit_textbox_players(request: Request, item_id: int, name: str = Query(...)):
    return dep._get_edit_textbox(request, "players", item_id, name)


@router.get("/{item_id}/cancel-edit")
def cancel_edit_players(request: Request, item_id: int, name: str = Query(...)):
    return dep._cancel_edit(request, "players", item_id, name)


@router.post("")
def create_player(request: Request, name: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if dep.check_entity_name_duplication(request, olympiad_id, "players", 0, name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "players")

    if result == dep.Status.SUCCESS:
        inserted_row = conn.execute(
            "INSERT INTO players (name, olympiad_id) VALUES (?, ?) RETURNING id, name, version",
            (name, olympiad_id)
        ).fetchone()

        conn.execute(
            "INSERT INTO participants (player_id, team_id) VALUES (?, ?)",
            (inserted_row["id"], None)
        )

        item = {
            "id": inserted_row["id"],
            "name": inserted_row["name"],
            "version": inserted_row["version"]
        }
        html_content = dep.templates.env.get_template(
            "entity_macros.html"
        ).module.entity_element(item, "players")
        num_players = conn.execute(
            "SELECT COUNT(*) FROM players WHERE olympiad_id = ?", (olympiad_id,)
        ).fetchone()[0]
        macros = dep.templates.env.get_template("entity_macros.html").module
        html_content += macros.num_players_label_oob(num_players)

        extra_headers["HX-Retarget"] = "#olympiad-players-list-items"
        extra_headers["HX-Reswap"] = "afterbegin"

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_olympiad_events(conn, olympiad_id, "enrollment-update")
        dep.notify_olympiad_page(olympiad_id, "player-created", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.get("/{player_id}")
def select_player(request: Request, player_id: int, player_name: str = Query(None, alias="name")):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    player = conn.execute(
        "SELECT id, name FROM players WHERE id = ?",
        (player_id,)
    ).fetchone()

    html_content = dep.render_player_fragment(
        "player_page",
        player_id=player["id"],
        player_name=player["name"],
        olympiad_id=olympiad_id,
        tab_id=request.headers.get("X-Tab-Id", ""),
    )

    return HTMLResponse(html_content)


@router.put("/{entity_id}")
def rename_players(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    conn = request.state.conn
    olympiad_id = dep.get_olympiad_from_request(request)["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if dep.check_entity_name_duplication(request, olympiad_id, "players", 0, new_name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED
    if result == dep.Status.SUCCESS and dep.check_player_in_running_event(request, entity_id):
        result = dep.Status.PLAYER_IN_RUNNING_EVENT

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "players")

    if result == dep.Status.SUCCESS:
        updated_row = conn.execute(
            "UPDATE players SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (new_name, entity_id)
        ).fetchone()
        item = {"id": entity_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = dep.templates.env.get_template("entity_macros.html").module.entity_element(item, "players")

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_olympiad_events(conn, olympiad_id, "enrollment-update")
        dep.notify_olympiad_page(olympiad_id, "player-renamed", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.delete("/{entity_id}")
def delete_players(request: Request, entity_id: int, entity_name: str = Query(..., alias="name")):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED
    if result == dep.Status.SUCCESS and dep.check_player_in_running_event(request, entity_id):
        result = dep.Status.PLAYER_IN_RUNNING_EVENT

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "players")

    if result == dep.Status.SUCCESS:
        conn.execute("DELETE FROM players WHERE id = ?", (entity_id,))
        html_content = dep.templates.get_template("entity_delete.html").render()

    html_content += dep._oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_olympiad_events(conn, olympiad_id, "enrollment-update")
        dep.notify_olympiad_page(olympiad_id, "player-deleted", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response
