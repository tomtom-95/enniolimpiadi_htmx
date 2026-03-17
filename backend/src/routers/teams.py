from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse

from ..internal import dependencies as dep

router = APIRouter(prefix="/api/teams")


@router.get("")
def list_teams(request: Request):
    conn = request.state.conn
    olympiad_id = dep.get_olympiad_from_request(request)["id"]
    items = conn.execute(
        "SELECT id, name, version FROM teams WHERE olympiad_id = ?", (olympiad_id,)
    ).fetchall()
    html_content = dep.render_entity_fragment(
        "entity_list", entities="teams", placeholder="Aggiungi un nuovo team", items=items
    )
    return HTMLResponse(html_content)


@router.get("/{item_id}/edit")
def get_edit_textbox_teams(request: Request, item_id: int, name: str = Query(...)):
    return dep._get_edit_textbox(request, "teams", item_id, name)


@router.get("/{item_id}/cancel-edit")
def cancel_edit_teams(request: Request, item_id: int, name: str = Query(...)):
    return dep._cancel_edit(request, "teams", item_id, name)


@router.post("")
def create_team(request: Request, name: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]
    olympiad_name = olympiad_badge_ctx["name"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_olympiad_exist(request, olympiad_id):
        result = dep.Status.OLYMPIAD_NOT_FOUND
    if result == dep.Status.SUCCESS and not dep.check_olympiad_name(request, olympiad_id, olympiad_name):
        result = dep.Status.OLYMPIAD_RENAMED
    if result == dep.Status.SUCCESS and dep.check_entity_name_duplication(request, olympiad_id, "teams", 0, name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "teams")

    if result == dep.Status.SUCCESS:
        inserted_row = conn.execute(
            "INSERT INTO teams (name, olympiad_id) VALUES (?, ?) RETURNING id, name, version",
            (name, olympiad_id)
        ).fetchone()

        conn.execute(
            "INSERT INTO participants (player_id, team_id) VALUES (?, ?)",
            (None, inserted_row["id"])
        )

        item = {"id": inserted_row["id"], "name": inserted_row["name"], "version": inserted_row["version"]}
        html_content = dep.templates.env.get_template("entity_macros.html").module.entity_element(item, "teams")
        extra_headers["HX-Retarget"] = "#entity-list"
        extra_headers["HX-Reswap"] = "afterbegin"

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@router.put("/{entity_id}")
def rename_teams(request: Request, entity_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    conn = request.state.conn
    olympiad_id = dep.get_olympiad_from_request(request)["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if dep.check_entity_name_duplication(request, olympiad_id, "teams", 0, new_name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "teams")

    if result == dep.Status.SUCCESS:
        updated_row = conn.execute(
            "UPDATE teams SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (new_name, entity_id)
        ).fetchone()
        item = {"id": entity_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = dep.templates.env.get_template("entity_macros.html").module.entity_element(item, "teams")

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@router.delete("/{entity_id}")
def delete_teams(request: Request, entity_id: int, entity_name: str = Query(..., alias="name")):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "teams")

    if result == dep.Status.SUCCESS:
        conn.execute("DELETE FROM teams WHERE id = ?", (entity_id,))
        html_content = dep.templates.get_template("entity_delete.html").render()

    html_content += dep._oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response
