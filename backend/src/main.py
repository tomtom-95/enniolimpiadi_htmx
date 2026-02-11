import os
import html
import json
import secrets
from enum import Enum
from itertools import combinations
from contextlib import asynccontextmanager
from collections import defaultdict, deque

import sqlite3

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

class EntityType(Enum):
    olympiads = "olympiads"
    players = "players"
    events = "events"
    teams = "teams"

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


def build_single_elimination_stage(request, conn, stage_id, stage_label):
    """Build a single-elimination stage dict from DB data."""

    # Load all bracket matches for this stage
    rows = conn.execute(
        "SELECT m.id AS match_id, bm.next_match_id "
        "FROM groups g "
        "JOIN matches m ON m.group_id = g.id "
        "JOIN bracket_matches bm ON bm.match_id = m.id "
        "WHERE g.event_stage_id = ?",
        (stage_id,)
    ).fetchall()

    if not rows:
        return {"name": stage_label, "rounds": [], "id": stage_id}

    # Load participants for all these matches
    match_ids = [r["match_id"] for r in rows]
    placeholders = ",".join("?" * len(match_ids))
    mp_rows = conn.execute(
        f"SELECT mp.match_id, mp.participant_id, "
        f"  COALESCE(pl.name, t.name) AS display_name "
        f"FROM match_participants mp "
        f"JOIN participants p ON p.id = mp.participant_id "
        f"LEFT JOIN players pl ON pl.id = p.player_id "
        f"LEFT JOIN teams t ON t.id = p.team_id "
        f"WHERE mp.match_id IN ({placeholders})",
        match_ids
    ).fetchall()

    # match_id -> list of (participant_id, name)
    match_parts = defaultdict(list)
    for r in mp_rows:
        match_parts[r["match_id"]].append((r["participant_id"], r["display_name"]))

    # Load scores
    score_rows = conn.execute(
        f"SELECT match_id, participant_id, score "
        f"FROM match_participant_scores "
        f"WHERE match_id IN ({placeholders})",
        match_ids
    ).fetchall()
    score_map = {}
    for r in score_rows:
        score_map[(r["match_id"], r["participant_id"])] = r["score"]

    # Build bracket tree
    feeders = defaultdict(list)
    matches_by_id = {}
    for r in rows:
        matches_by_id[r["match_id"]] = r
        if r["next_match_id"] is not None:
            feeders[r["next_match_id"]].append(r["match_id"])

    # Find the final (next_match_id IS NULL)
    final_id = None
    for mid, r in matches_by_id.items():
        if r["next_match_id"] is None:
            final_id = mid
            break

    # BFS to assign round depths (0 = final)
    round_assignment = {}
    queue = deque([(final_id, 0)])
    while queue:
        mid, depth = queue.popleft()
        round_assignment[mid] = depth
        for feeder_id in feeders.get(mid, []):
            queue.append((feeder_id, depth + 1))

    # Group by round, earliest rounds first
    max_round = max(round_assignment.values()) if round_assignment else 0
    rounds_list = []
    for r in range(max_round, -1, -1):
        mids_in_round = [mid for mid, rn in round_assignment.items() if rn == r]

        match_dicts = []
        for mid in mids_in_round:
            parts = match_parts.get(mid, [])
            p1 = parts[0][1] if len(parts) > 0 else "?"
            p2 = parts[1][1] if len(parts) > 1 else "?"
            s1 = score_map.get((mid, parts[0][0])) if len(parts) > 0 else None
            s2 = score_map.get((mid, parts[1][0])) if len(parts) > 1 else None
            if s1 is not None and s2 is not None:
                score = f"{s1} - {s2}"
            else:
                score = "- vs -"
            match_dicts.append({"p1": p1, "p2": p2, "score": score})

        rounds_list.append({"matches": match_dicts})
    
    res = {
        "name": stage_label, "rounds": rounds_list, "id": stage_id
    }

    return res


def derive_event_status(current_stage_order, max_stage_order):
    """Derive the event's display status from current_stage_order.

    - NULL → 'registration'
    - <= max_stage_order → 'started'
    - > max_stage_order → 'finished'
    """
    if current_stage_order is None:
        return "registration"
    if max_stage_order is not None and current_stage_order > max_stage_order:
        return "finished"
    return "started"


def build_groups_stage(request, conn, stage_id: int, stage_label):
    """Build and render a groups stage for display."""

    # Build groups data
    group_rows = conn.execute(
        "SELECT id FROM groups WHERE event_stage_id = ? ORDER BY id",
        (stage_id,)
    ).fetchall()

    groups = []
    for idx, grow in enumerate(group_rows):
        gid = grow["id"]

        # Participant names in seed order
        part_rows = conn.execute(
            "SELECT gp.participant_id, COALESCE(pl.name, t.name) AS display_name "
            "FROM group_participants gp "
            "JOIN participants p ON p.id = gp.participant_id "
            "LEFT JOIN players pl ON pl.id = p.player_id "
            "LEFT JOIN teams t ON t.id = p.team_id "
            "WHERE gp.group_id = ? ORDER BY gp.seed",
            (gid,)
        ).fetchall()

        participants = [r["display_name"] for r in part_rows]
        pid_to_name = {r["participant_id"]: r["display_name"] for r in part_rows}

        match_rows = conn.execute(
            "SELECT m.id, "
            "  mp1.participant_id AS p1_id, mp2.participant_id AS p2_id, "
            "  mps1.score AS p1_score, mps2.score AS p2_score "
            "FROM matches m "
            "JOIN match_participants mp1 ON mp1.match_id = m.id "
            "JOIN match_participants mp2 ON mp2.match_id = m.id "
            "  AND mp2.participant_id > mp1.participant_id "
            "LEFT JOIN match_participant_scores mps1 "
            "  ON mps1.match_id = m.id AND mps1.participant_id = mp1.participant_id "
            "LEFT JOIN match_participant_scores mps2 "
            "  ON mps2.match_id = m.id AND mps2.participant_id = mp2.participant_id "
            "WHERE m.group_id = ?",
            (gid,)
        ).fetchall()

        scores = {}
        for mr in match_rows:
            p1_name = pid_to_name.get(mr["p1_id"])
            p2_name = pid_to_name.get(mr["p2_id"])
            if p1_name and p2_name:
                score_str = (
                    f"{mr['p1_score']} - {mr['p2_score']}"
                    if mr["p1_score"] is not None and mr["p2_score"] is not None
                    else None
                )
                scores.setdefault(p1_name, {})[p2_name] = score_str

        groups.append({
            "name": f"Girone {chr(65 + idx)}",
            "participants": participants,
            "scores": scores,
        })

    total_participants = sum(len(g["participants"]) for g in groups)

    stage = {
        "name": stage_label,
        "groups": groups,
        "id": stage_id,
        "total_participants": total_participants
    }

    return stage


def trigger_badge_update(response: Response) -> None:
    """Add HX-Trigger header to make the badge refresh itself."""
    response.headers["HX-Trigger"] = "badgeChanged"


def get_session_data(conn, session_id: str):
    """Load session data for the given session ID."""
    return conn.execute(
        "SELECT selected_olympiad_id, selected_olympiad_version FROM sessions WHERE id = ?",
        (session_id,)
    ).fetchone()


def diagnose_entity_noop(
    request: Request,
    entities: EntityType,
    entity_id: int,
    expected_version: int,
    olympiad_id: int,
    session_id: str,
    conn,
    action_type: str,
    action_params: dict
) -> Response:
    """
    Diagnose why an entity operation was a no-op.

    Checks: entity deleted, olympiad deleted (cascade), version mismatch, or not authorized.
    """
    diag = conn.execute(
        f"""
        SELECT
          e.id IS NOT NULL as entity_exists,
          e.version as current_version,
          e.name as current_name,
          o.id IS NOT NULL as olympiad_exists,
          soa.session_id IS NOT NULL as is_authorized
        FROM (SELECT 1) dummy
        LEFT JOIN {entities.value} e ON e.id = ?
        LEFT JOIN olympiads o ON o.id = ?
        LEFT JOIN session_olympiad_auth soa
          ON soa.session_id = ? AND soa.olympiad_id = ?
        """,
        (entity_id, olympiad_id, session_id, olympiad_id)
    ).fetchone()

    # Case 1: Entity doesn't exist
    if not diag["entity_exists"]:
        # Check if olympiad was also deleted (cascade delete)
        if not diag["olympiad_exists"]:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                (session_id,)
            )
            conn.commit()
            response = templates.TemplateResponse(request, "olympiad_not_found.html")
            response.headers["HX-Retarget"] = "#main-content"
            response.headers["HX-Reswap"] = "innerHTML"
            trigger_badge_update(response)
            return response

        # Just the entity was deleted
        html_content = templates.get_template("entity_deleted_oob.html").render()
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities.value}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        return response

    # Case 2: Version mismatch - entity was modified
    if diag["current_version"] != expected_version:
        item = {"id": entity_id, "name": diag["current_name"], "version": diag["current_version"]}
        html_content = templates.get_template("entity_renamed_oob.html").render(
            item=item, entities=entities.value, hx_target="#main-content"
        )
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities.value}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        return response

    # Case 3: Not authorized - show PIN modal
    response = templates.TemplateResponse(
        request, "pin_modal.html", {
            "action": "/api/validate_pin",
            "action_type": action_type,
            "olympiad_id": olympiad_id,
            "params": action_params
        }
    )
    response.headers["HX-Retarget"] = "#modal-container"
    response.headers["HX-Reswap"] = "innerHTML"
    return response


def diagnose_olympiad_noop(
    request: Request,
    olympiad_id: int,
    expected_version: int,
    session_id: str,
    session_data,
    conn,
    action_type: str,
    action_params: dict
) -> Response:
    """
    Diagnose why an olympiad operation was a no-op and return appropriate response.

    Called after a conditional UPDATE/DELETE returned no rows.
    Checks: deleted, version mismatch, or not authorized.
    """
    diag = conn.execute(
        """
        SELECT
          o.id IS NOT NULL as olympiad_exists,
          o.version as current_version,
          o.name as current_name,
          soa.session_id IS NOT NULL as is_authorized
        FROM (SELECT 1) dummy
        LEFT JOIN olympiads o ON o.id = ?
        LEFT JOIN session_olympiad_auth soa
          ON soa.session_id = ? AND soa.olympiad_id = ?
        """,
        (olympiad_id, session_id, olympiad_id)
    ).fetchone()

    if not diag["olympiad_exists"]:
        # Olympiad was deleted
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                (session_id,)
            )
            conn.commit()

        html_content = templates.get_template("entity_deleted_oob.html").render()
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        trigger_badge_update(response)
        return response

    if diag["current_version"] != expected_version:
        # Version mismatch - olympiad was modified
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
                (diag["current_version"], session_id)
            )
            conn.commit()

        item = {"id": olympiad_id, "name": diag["current_name"], "version": diag["current_version"]}
        html_content = templates.get_template("entity_renamed_oob.html").render(
            item=item, entities="olympiads", hx_target="#olympiad-badge-container"
        )
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        trigger_badge_update(response)
        return response

    # Not authorized - show PIN modal
    response = templates.TemplateResponse(
        request, "pin_modal.html", {
            "action": "/api/validate_pin",
            "action_type": action_type,
            "olympiad_id": olympiad_id,
            "params": action_params
        }
    )
    response.headers["HX-Retarget"] = "#modal-container"
    response.headers["HX-Reswap"] = "innerHTML"
    return response


def get_db():
    conn = database.get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()

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


@app.get("/api/olympiads")
async def list_olympiads(request: Request, conn = Depends(get_db)):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)

    if session_data["selected_olympiad_id"] is not None:
        olympiad = conn.execute(
            "SELECT id, name, version FROM olympiads WHERE id = ?",
            (session_data["selected_olympiad_id"],)
        ).fetchone()

        if not olympiad:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                (session_id,)
            )
            conn.commit()
            response = templates.TemplateResponse(request, "olympiad_not_found.html")
            trigger_badge_update(response)
            return response

        if olympiad["version"] != session_data["selected_olympiad_version"]:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
                (olympiad["version"], session_id)
            )
            conn.commit()
            response = templates.TemplateResponse(
                request, "olympiad_name_changed.html",
                {"olympiad_name": olympiad["name"]}
            )
            trigger_badge_update(response)
            return response

    cursor = conn.execute("SELECT id, name, version FROM olympiads")
    rows = [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
    placeholder = entity_list_form_placeholder["olympiads"]
    return templates.TemplateResponse(
        request, "entity_list.html",
        {"entities": "olympiads", "placeholder": placeholder, "items": rows}
    )


@app.get("/api/olympiads/{olympiad_id}/{olympiad_version}")
async def select_olympiad(
    request: Request,
    olympiad_id: int,
    olympiad_version: int,
    conn = Depends(get_db)
):
    """Select an olympiad and update the olympiad badge"""
    session_id = request.state.session_id

    olympiad = conn.execute("SELECT id, name, version FROM olympiads WHERE id = ?", (olympiad_id,)).fetchone()

    if not olympiad:
        response = templates.TemplateResponse(request, "olympiad_not_found.html")
        response.headers["HX-Retarget"] = "#main-content"
        return response

    if olympiad["version"] != olympiad_version:
        response = templates.TemplateResponse(
            request, "olympiad_name_changed.html", {"olympiad_name": olympiad["name"]}
        )
        response.headers["HX-Retarget"] = "#main-content"
        return response

    # Update session with the newly selected olympiad
    conn.execute(
        "UPDATE sessions SET (selected_olympiad_id, selected_olympiad_version) = (?, ?) WHERE id = ?",
        (olympiad["id"], olympiad["version"], session_id)
    )
    conn.commit()

    return templates.TemplateResponse(
        request, "olympiad_badge.html",
        {"olympiad": {"id": olympiad["id"], "name": olympiad["name"], "version": olympiad["version"]}}
    )


@app.post("/api/olympiads")
async def create_olympiad(
    request: Request,
    name: str = Form(None),
    pin: str = Form(None),
    params: str = Form(None),
    conn = Depends(get_db)
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


@app.put("/api/olympiads/{olympiad_id}/{olympiad_version}")
async def rename_olympiad(
    request: Request,
    olympiad_id: int,
    olympiad_version: int,
    name: str = Form(...),
    conn = Depends(get_db)
):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)

    try:
        updated_row = conn.execute(
            """
            UPDATE olympiads
            SET name = ?, version = version + 1
            WHERE id = ?
              AND version = ?
              AND EXISTS (
                SELECT 1 FROM session_olympiad_auth
                WHERE session_id = ? AND olympiad_id = ?
              )
            RETURNING id, name, version
            """,
            (name, olympiad_id, olympiad_version, session_id, olympiad_id)
        ).fetchone()
    except sqlite3.IntegrityError:
        # Duplicate name
        response = HTMLResponse(templates.get_template("olympiad_name_duplicate.html").render())
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    if updated_row:
        # Success - update session version if this is the selected olympiad
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_version = ? WHERE id = ?",
                (updated_row["version"], session_id)
            )
        conn.commit()

        item = {"id": olympiad_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = templates.get_template("entity_element.html").render(
            item=item, entities="olympiads", hx_target="#olympiad-badge-container"
        )
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        trigger_badge_update(response)
        return response

    # Update was a no-op - diagnose why
    return diagnose_olympiad_noop(
        request, olympiad_id, olympiad_version, session_id, session_data, conn,
        "rename_olympiad", {"name": name, "version": olympiad_version}
    )


@app.delete("/api/olympiads/{olympiad_id}/{olympiad_version}")
async def delete_olympiad(
    request: Request,
    olympiad_id: int,
    olympiad_version: int,
    conn = Depends(get_db)
):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)

    deleted_row = conn.execute(
        """
        DELETE FROM olympiads
        WHERE id = ?
          AND version = ?
          AND EXISTS (
            SELECT 1 FROM session_olympiad_auth
            WHERE session_id = ? AND olympiad_id = ?
          )
        RETURNING id
        """,
        (olympiad_id, olympiad_version, session_id, olympiad_id)
    ).fetchone()

    if deleted_row:
        # Success - clear session selection if this was the selected olympiad
        if session_data["selected_olympiad_id"] == olympiad_id:
            conn.execute(
                "UPDATE sessions SET selected_olympiad_id = NULL, selected_olympiad_version = NULL WHERE id = ?",
                (session_id,)
            )
        conn.commit()

        html_content = templates.get_template("entity_delete.html").render()
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#olympiads-{olympiad_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        trigger_badge_update(response)
        return response

    # Delete was a no-op - diagnose why
    return diagnose_olympiad_noop(
        request, olympiad_id, olympiad_version, session_id, session_data, conn,
        "delete_olympiad", {"version": olympiad_version}
    )


@app.get("/api/{entities}/{item_id}/{version}/edit")
async def get_edit_textbox(
    request: Request,
    entities: EntityType,
    item_id: int,
    version: int,
    name: str
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
    request: Request,
    entities: EntityType,
    item_id: int,
    version: int,
    name: str,
):
    hx_target = "#olympiad-badge-container" if entities == "olympiads" else "#main-content"
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



@app.get("/api/events/{event_id}/players")
async def get_event_players(
    request: Request,
    event_id: int,
    conn = Depends(get_db)
):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)
    olympiad_id = session_data["selected_olympiad_id"]

    enrolled_players = conn.execute(
        """
        SELECT DISTINCT p.id, COALESCE(pl.name, t.name) AS name
        FROM participants p
        LEFT JOIN players pl ON pl.id = p.player_id
        LEFT JOIN teams t ON t.id = p.team_id
        JOIN group_participants gp ON gp.participant_id = p.id
        JOIN groups g ON g.id = gp.group_id
        JOIN event_stages es ON es.id = g.event_stage_id
        WHERE es.event_id = ?
        ORDER BY name
        """,
        (event_id,)
    ).fetchall()
    enrolled_participant_ids = {p["id"] for p in enrolled_players}

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
    available_players = [p for p in all_participants if p["id"] not in enrolled_participant_ids]

    return templates.TemplateResponse(
        request, "event_players_section.html",
        {
            "enrolled_players": enrolled_players,
            "available_players": available_players,
            "event_id": event_id,
        }
    )


@app.get("/api/events/{event_id}/{event_version}")
async def select_event(
    request: Request,
    event_id: int,
    event_version: int,
    conn = Depends(get_db)
):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)
    olympiad_id = session_data["selected_olympiad_id"]

    event = conn.execute(
        f"""
        SELECT id, name, version, current_stage_order, score_kind
        FROM events
        WHERE id = ?
            AND version = ?
            AND olympiad_id = ?""",
        (event_id, event_version, olympiad_id)
    ).fetchone()

    if event:
        max_stage = conn.execute(
            "SELECT MAX(stage_order) AS max_order FROM event_stages WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        max_stage_order = max_stage["max_order"] if max_stage else None

        event_status = utils.derive_event_status(
            event["current_stage_order"],
            max_stage_order
        )

        return templates.TemplateResponse(
            request, "event_page.html",
            {
                "event": {
                    "id": event["id"],
                    "name": event["name"],
                    "version": event["version"],
                    "status": event_status,
                },
            }
        )
    else:
        return diagnose_entity_noop(
            request, EntityType.events, event_id, event_version, olympiad_id, session_id, conn,
            "", {}
        )


@app.get("/api/events/{event_id}/stage/{stage_order}")
async def get_event_stage(
    request: Request,
    event_id: int,
    stage_order: int,
    conn = Depends(get_db)
):
    row = conn.execute(
        """
        SELECT
            e.current_stage_order,
            es.id,
            es.stage_order,
            es.kind,
            sk.label AS name
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
    stage_label = row["name"]

    # Get total number of stages for navigation
    total_stages = conn.execute(
        "SELECT COUNT(*) AS count FROM event_stages WHERE event_id = ?",
        (event_id,)
    ).fetchone()["count"]

    if stage_kind == "groups":
        stage = utils.build_groups_stage(request, conn, stage_id, stage_label)
    elif stage_kind == "round_robin":
        return HTMLResponse("<div class='error-banner'>Fase non trovata</div>")
    elif stage_kind == "single_elimination":
        stage = utils.build_single_elimination_stage(request, conn, stage_id, stage_label)
    
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
async def resize_stage_groups(
    request: Request,
    event_id: int,
    stage_id: int,
    num_groups: int = Form(...),
    conn = Depends(get_db)
):
    event_row = conn.execute(
        "SELECT current_stage_order FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    if not event_row or event_row["current_stage_order"] is not None:
        return HTMLResponse("<div class='error-banner'>Resize consentito solo durante la registrazione</div>")

    stage_row = conn.execute(
        "SELECT id, stage_order, kind FROM event_stages WHERE id = ? AND event_id = ?",
        (stage_id, event_id)
    ).fetchone()
    if not stage_row or stage_row["kind"] not in ("groups", "round_robin"):
        return HTMLResponse("<div class='error-banner'>Fase non trovata o già iniziata</div>")

    participant_rows = conn.execute(
        "SELECT DISTINCT gp.participant_id "
        "FROM group_participants gp "
        "JOIN groups g ON g.id = gp.group_id "
        "WHERE g.event_stage_id = ? "
        "ORDER BY gp.participant_id",
        (stage_id,)
    ).fetchall()
    participant_ids = [r["participant_id"] for r in participant_rows]
    total = len(participant_ids)

    if total < 2:
        return HTMLResponse("<div class='error-banner'>Servono almeno 2 partecipanti</div>")

    num_groups = max(1, min(num_groups, total // 2))

    # Teardown: CASCADE handles group_participants, matches, match_participants, scores
    conn.execute("DELETE FROM groups WHERE event_stage_id = ?", (stage_id,))

    # Rebuild groups
    buckets = [[] for _ in range(num_groups)]
    for i, pid in enumerate(participant_ids):
        buckets[i % num_groups].append(pid)

    for bucket in buckets:
        grow = conn.execute(
            "INSERT INTO groups (event_stage_id) VALUES (?) RETURNING id",
            (stage_id,)
        ).fetchone()
        gid = grow["id"]

        for seed, pid in enumerate(bucket):
            conn.execute(
                "INSERT INTO group_participants (group_id, participant_id, seed) VALUES (?, ?, ?)",
                (gid, pid, seed)
            )

        for p1, p2 in combinations(bucket, 2):
            mrow = conn.execute(
                "INSERT INTO matches (group_id) VALUES (?) RETURNING id",
                (gid,)
            ).fetchone()
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (mrow["id"], p1)
            )
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (mrow["id"], p2)
            )

    conn.commit()

    # Re-render only the groups section
    stage = utils.build_groups_stage(request, conn, stage_id, "Tusorella")

    return templates.TemplateResponse(
        request, "stage_groups.html",
        {
            "stage": stage,
            "event_id": event_id
        }
    )


@app.put("/api/{entities}/{entity_id}/{entity_version}")
async def rename_entity(
    request: Request,
    entities: EntityType,
    entity_id: int,
    entity_version: int,
    name: str = Form(...),
    conn = Depends(get_db)
):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)
    olympiad_id = session_data["selected_olympiad_id"]

    # Update only if correct version, belongs to selected olympiad, and session is authorized
    try:
        updated_row = conn.execute(
            f"""
            UPDATE {entities.value}
            SET name = ?, version = version + 1
            WHERE id = ?
              AND version = ?
              AND olympiad_id = ?
              AND EXISTS (
                SELECT 1 FROM session_olympiad_auth
                WHERE session_id = ? AND olympiad_id = ?
              )
            RETURNING id, name, version
            """,
            (name, entity_id, entity_version, olympiad_id, session_id, olympiad_id)
        ).fetchone()
    except sqlite3.IntegrityError:
        # Duplicate name within the olympiad
        response = HTMLResponse(templates.get_template("entity_name_duplicate.html").render())
        response.headers["HX-Retarget"] = "#modal-container"
        response.headers["HX-Reswap"] = "innerHTML"
        return response

    if updated_row:
        conn.commit()
        item = {"id": entity_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = templates.get_template("entity_element.html").render(
            item=item, entities=entities.value, hx_target="#main-content"
        )
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities.value}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        return response

    # Update was a no-op - diagnose why
    return diagnose_entity_noop(
        request, entities, entity_id, entity_version, olympiad_id, session_id, conn,
        f"rename_{entities.value}",
        {"entity_id": entity_id, "name": name, "version": entity_version, "entities": entities.value}
    )


@app.delete("/api/{entities}/{entity_id}/{entity_version}")
async def delete_entity(
    request: Request,
    entities: EntityType,
    entity_id: int,
    entity_version: int,
    conn = Depends(get_db)
):
    session_id = request.state.session_id
    session_data = get_session_data(conn, session_id)

    olympiad_id = session_data["selected_olympiad_id"]

    # Delete only if correct version belongs to selected olympiad and session is authorized
    deleted_row = conn.execute(
        f"""
        DELETE FROM {entities.value}
        WHERE id = ?
          AND version = ?
          AND olympiad_id = ?
          AND EXISTS (
            SELECT 1 FROM session_olympiad_auth
            WHERE session_id = ? AND olympiad_id = ?
          )
        RETURNING id
        """,
        (entity_id, entity_version, olympiad_id, session_id, olympiad_id)
    ).fetchone()

    if deleted_row:
        conn.commit()
        html_content = templates.get_template("entity_delete.html").render()
        response = HTMLResponse(html_content)
        response.headers["HX-Retarget"] = f"#{entities.value}-{entity_id}"
        response.headers["HX-Reswap"] = "outerHTML"
        return response

    # Delete was a no-op - diagnose why
    return diagnose_entity_noop(
        request, entities, entity_id, entity_version, olympiad_id, session_id, conn,
        f"delete_{entities.value}",
        {"entity_id": entity_id, "version": entity_version, "entities": entities.value}
    )


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
    parsed_params = json.loads(params)

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
        # PIN correct, auth granted - call the actual endpoint function
        conn.commit()
        return await _dispatch_action(request, action_type, olympiad_id, parsed_params, conn)

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

    # Olympiad doesn't exist - call endpoint anyway, it will handle the deleted case
    return await _dispatch_action(request, action_type, olympiad_id, parsed_params, conn)


async def _dispatch_action(request: Request, action_type: str, olympiad_id: int, params: dict, conn):
    """Dispatch to the actual endpoint function after PIN validation."""
    if action_type == "rename_olympiad":
        response = await rename_olympiad(request, olympiad_id, params["version"], params["name"], conn)
    elif action_type == "delete_olympiad":
        response = await delete_olympiad(request, olympiad_id, params["version"], conn)
    elif action_type == "delete_players" or action_type == "delete_teams" or action_type == "delete_events":
        entities = EntityType(params["entities"])
        response = await delete_entity(request, entities, params["entity_id"], params["version"], conn)
    elif action_type == "rename_players" or action_type == "rename_teams" or action_type == "rename_events":
        entities = EntityType(params["entities"])
        response = await rename_entity(request, entities, params["entity_id"], params["version"], params["name"], conn)
    else:
        response = HTMLResponse("")
        response.headers["HX-Reswap"] = "none"

    response.headers["HX-Trigger-After-Settle"] = "closeModal"
    return response


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8080)
