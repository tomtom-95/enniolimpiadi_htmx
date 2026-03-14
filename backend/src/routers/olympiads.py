import asyncio

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, StreamingResponse

from ..internal import dependencies as dep

router = APIRouter(prefix="/api/olympiads")


@router.get("/{olympiad_id}/sse")
async def olympiad_sse(request: Request, olympiad_id: int, tab_id: str = Query("")):
    queue: asyncio.Queue = asyncio.Queue()
    entry = (tab_id, queue)
    dep._olympiad_subscribers[olympiad_id].add(entry)

    async def generate():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield data
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            dep._olympiad_subscribers[olympiad_id].discard(entry)

    media_type = "text/event-stream"
    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(generate(), media_type=media_type, headers=headers)


@router.get("")
def list_olympiads(request: Request):
    conn = request.state.conn

    placeholder = "Aggiungi un olympiade"
    cursor = conn.execute("SELECT id, name, version FROM olympiads")
    rows = [
        {"id": row["id"], "name": row["name"], "version": row["version"]}
        for row in cursor.fetchall()
    ]
    html_content = dep.render_entity_fragment(
        "entity_list", entities="olympiads", placeholder=placeholder, items=rows
    )
    return HTMLResponse(html_content)


@router.get("/create")
def get_create_olympiad_modal(request: Request, name: str = Query(...)):
    template_ctx = {"params": {"name": name}}
    return dep.templates.TemplateResponse(request, "pin_modal.html", template_ctx)


@router.post("")
def create_olympiad(request: Request, pin: str = Form(...), name: str = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if dep.check_olympiad_name_duplication(request, 0, name):
        result = dep.Status.NAME_DUPLICATION

    if result == dep.Status.SUCCESS and len(pin) != 4:
        result = dep.Status.INVALID_PIN

    extra_headers = {}

    if result == dep.Status.INVALID_PIN:
        error_message = "Il PIN deve essere composto da 4 cifre"
        html_content = dep.templates.get_template("pin_modal.html")
        html_content = html_content.render(params={"name": name}, error=error_message)
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"

    if result == dep.Status.NAME_DUPLICATION:
        html_content = dep.render_modal_fragment("name_duplicate", entities="olympiads")
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"

    if result == dep.Status.SUCCESS:
        row = conn.execute(
            f"INSERT INTO olympiads (name, pin) VALUES (?, ?) RETURNING id", (name, pin)
        ).fetchone()
        olympiad_id = row[0]

        conn.execute(
            f"INSERT INTO session_olympiad_auth (session_id, olympiad_id) VALUES (?, ?)",
            (session_id, olympiad_id)
        )
        item = {"id": olympiad_id, "name": name}
        html_content = "".join([
            dep.templates.env.get_template("entity_macros.html").module.entity_element(item, "olympiads"),
            '<div id="modal-container" hx-swap-oob="innerHTML"></div>'
        ])

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@router.get("/{olympiad_id}")
def select_olympiad(request: Request, olympiad_id: int, olympiad_name: str = Query(..., alias="name")):
    conn = request.state.conn

    result = dep.Status.SUCCESS
    if not dep.check_olympiad_exist(request, olympiad_id):
        result = dep.Status.OLYMPIAD_NOT_FOUND
    if result == dep.Status.SUCCESS and not dep.check_olympiad_name(request, olympiad_id, olympiad_name):
        result = dep.Status.OLYMPIAD_RENAMED

    extra_headers = {}

    if result == dep.Status.OLYMPIAD_NOT_FOUND:
        extra_headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
        extra_headers["HX-Reswap"] = "outerHTML"
        html_content = dep.render_entity_fragment("entity_deleted_oob")
    
    if result != dep.Status.OLYMPIAD_NOT_FOUND:
        olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()
        olympiad_data = {"id": olympiad_id, "name": olympiad["name"]}

    if result == dep.Status.OLYMPIAD_RENAMED:
        extra_headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
        extra_headers["HX-Reswap"] = "outerHTML"
        html_content = dep.render_entity_fragment(
            "entity_renamed_oob", entities="olympiads", item=olympiad_data
        )
    
    if result == dep.Status.SUCCESS:
        tab_id = request.headers.get("X-Tab-Id", "")

        events = conn.execute(
            "SELECT id, name FROM events WHERE olympiad_id = ? ORDER BY name",
            (olympiad_id,)
        ).fetchall()
        players = conn.execute(
            "SELECT id, name FROM players WHERE olympiad_id = ? ORDER BY name",
            (olympiad_id,)
        ).fetchall()
        events = [{"id": r["id"], "name": r["name"]} for r in events]
        players = [{"id": r["id"], "name": r["name"]} for r in players]

        html_content = dep.templates.get_template("olympiad_page.html").render(
            olympiad=olympiad_data, events=events, players=players
        )
        html_content += dep.templates.get_template("olympiad_badge.html").render(
            olympiad=olympiad_data, tab_id=tab_id, oob=True
        )
        html_content += dep._oob_sse_link_html(olympiad_id, tab_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@router.get("/{olympiad_id}/edit")
def get_edit_textbox_olympiads(request: Request, olympiad_id: int, name: str = Query(...)):
    return dep._get_edit_textbox(request, "olympiads", olympiad_id, name)


@router.get("/{olympiad_id}/cancel-edit")
def cancel_edit_olympiads(request: Request, olympiad_id: int, name: str = Query(...)):
    return dep._cancel_edit(request, "olympiads", olympiad_id, name)


@router.put("/{olympiad_id}")
def rename_olympiad(
    request: Request,
    olympiad_id: int,
    olympiad_curr_name: str = Form(..., alias="curr_name"),
    olympiad_new_name: str = Form(..., alias="new_name")
):
    conn = request.state.conn
    conn.execute("BEGIN IMMEDIATE")

    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    result = dep.Status.SUCCESS
    if not dep.check_olympiad_exist(request, olympiad_id):
        result = dep.Status.OLYMPIAD_NOT_FOUND
    if result == dep.Status.SUCCESS and not dep.check_olympiad_name(request, olympiad_id, olympiad_curr_name):
        result = dep.Status.OLYMPIAD_RENAMED
    if result == dep.Status.SUCCESS and dep.check_olympiad_name_duplication(request, olympiad_id, olympiad_new_name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    extra_headers = {}

    if result == dep.Status.OLYMPIAD_NOT_FOUND:
        html_content = dep.render_entity_fragment("entity_deleted_oob")

    if result == dep.Status.OLYMPIAD_RENAMED:
        olympiad_badge_ctx = {"id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"]}
        html_content = dep.render_entity_fragment("entity_renamed_oob", entities="olympiads", item=olympiad_badge_ctx)

    if result == dep.Status.NOT_AUTHORIZED:
        extra_headers["HX-Pin-Required"] = "true"
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = dep.templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)

    if result == dep.Status.NAME_DUPLICATION:
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = dep.render_modal_fragment("name_duplicate", entities="olympiads")

    if result == dep.Status.SUCCESS:
        updated_row = conn.execute(
            "UPDATE olympiads SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (olympiad_new_name, olympiad_id)
        ).fetchone()
        item = {"id": olympiad_id, "name": updated_row["name"]}
        html_content = "".join([
            dep.templates.env.get_template("entity_macros.html").module.entity_element(item, "olympiads"),
            dep._oob_badge_html(request, olympiad_id)
        ])

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_olympiad(olympiad_id, "olympiad-renamed", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.delete("/{olympiad_id}")
def delete_olympiad(request: Request, olympiad_id: int, olympiad_name: str = Query(..., alias="name")):
    conn = request.state.conn
    conn.execute("BEGIN IMMEDIATE")

    olympiad = conn.execute("SELECT * FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    result = dep.Status.SUCCESS
    if not dep.check_olympiad_exist(request, olympiad_id):
        result = dep.Status.OLYMPIAD_NOT_FOUND
    if result == dep.Status.SUCCESS and not dep.check_olympiad_name(request, olympiad_id, olympiad_name):
        result = dep.Status.OLYMPIAD_RENAMED
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    extra_headers = {}
    if result == dep.Status.OLYMPIAD_NOT_FOUND:
        html_content = dep.render_entity_fragment("entity_deleted_oob")
    elif result == dep.Status.OLYMPIAD_RENAMED:
        olympiad_badge_ctx = {"id": olympiad_id, "name": olympiad["name"], "version": olympiad["version"]}
        html_content = dep.render_entity_fragment("entity_renamed_oob", entities="olympiads", item=olympiad_badge_ctx)
    elif result == dep.Status.NOT_AUTHORIZED:
        extra_headers["HX-Pin-Required"] = "true"
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
        html_content = dep.templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)
    else:
        conn.execute(
            "DELETE FROM olympiads WHERE id = ? AND name = ? RETURNING id",
            (olympiad_id, olympiad_name)
        ).fetchone()
        html_content = dep.templates.get_template("entity_delete.html").render()
        html_content += dep._oob_badge_html(request, olympiad_id)

    tab_id = request.headers.get("X-Tab-Id", "")
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_olympiad(olympiad_id, "olympiad-deleted", exclude_tab_id=tab_id)
        dep._olympiad_subscribers.pop(olympiad_id, None)
    else:
        conn.rollback()

    return response


@router.get("/{olympiad_id}/deleted-notice")
def get_olympiad_deleted_notice(request: Request, olympiad_id: int):
    tab_id = request.headers.get("X-Tab-Id", "")
    html_content = dep.render_modal_fragment("olympiad_deleted")
    html_content += dep.templates.get_template("olympiad_badge.html").render(
        olympiad=dep.sentinel_olympiad_badge, tab_id=tab_id, oob=True
    )
    html_content += dep._oob_sse_link_html(0, tab_id)
    return HTMLResponse(html_content)


@router.get("/{olympiad_id}/renamed-notice")
def get_olympiad_renamed_notice(request: Request, olympiad_id: int):
    tab_id = request.headers.get("X-Tab-Id", "")
    olympiad_data = request.state.conn.execute(
        "SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id,)
    ).fetchone()
    olympiad = {"id": olympiad_data["id"], "name": olympiad_data["name"], "version": olympiad_data["version"]}
    html_content = dep.render_modal_fragment("olympiad_renamed")
    html_content += dep.templates.get_template("olympiad_badge.html").render(
        olympiad=olympiad, tab_id=tab_id, oob=True
    )
    return HTMLResponse(html_content)
