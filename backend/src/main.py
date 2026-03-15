import secrets

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, Response

from . import database
from .routers import events, olympiads, players, teams
from .internal import dependencies as dep


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db(dep.db_path, dep.schema_path)
    yield
    dep.db_path.unlink()


app = FastAPI(lifespan=lifespan)

app.include_router(olympiads.router)
app.include_router(events.router)
app.include_router(players.router)
app.include_router(teams.router)


@app.middleware("http")
async def session_middleware(request: Request, call_next):
    session_id = request.cookies.get("session")
    conn = database.get_connection(dep.db_path)

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
    from fastapi.responses import JSONResponse
    return JSONResponse(200)


@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    tab_id = secrets.token_urlsafe(16)
    return dep.root_templates.TemplateResponse(request, "index.html", {"tab_id": tab_id})


@app.get("/index.css")
def serve_css():
    css_path = dep.root / "frontend" / "index.css"
    return Response(content=css_path.read_text(), media_type="text/css")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.post("/api/validate_pin")
def validate_pin(request: Request, pin: str = Form(...), olympiad_id: int = Form(...)):
    conn = request.state.conn
    session_id = request.state.session_id

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_olympiad_exist(request, olympiad_id):
        result = dep.Status.OLYMPIAD_NOT_FOUND

    if result == dep.Status.SUCCESS and not dep.check_pin_valid(request, olympiad_id, pin):
        result = dep.Status.INVALID_PIN

    extra_headers = {}
    if result == dep.Status.OLYMPIAD_NOT_FOUND:
        html_content = dep.render_modal_fragment("olympiad_not_found")
        extra_headers["HX-Retarget"] = "#modal-container"
        extra_headers["HX-Reswap"] = "innerHTML"
    elif result == dep.Status.INVALID_PIN:
        html_content = dep.templates.get_template("pin_modal.html").render(
            olympiad_id=olympiad_id, error="PIN errato"
        )
    else:
        conn.execute(
            """
            INSERT INTO session_olympiad_auth (session_id, olympiad_id)
            VALUES (?, ?)
            """,
            (session_id, olympiad_id)
        )
        auth_section_inner = dep.render_olympiad_fragment(
            "olympiad_auth_section", is_authorized=True, olympiad={"id": olympiad_id}
        )
        html_content = f'<div id="olympiad-auth-section" hx-swap-oob="innerHTML">{auth_section_inner}</div>'

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


if __name__ == "__main__":
    uvicorn.run("src.main:app", reload=True, host="0.0.0.0", port=8000, timeout_graceful_shutdown=1)
