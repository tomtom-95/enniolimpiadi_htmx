from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

app = FastAPI()

root = Path(__file__).parent.parent
templates = Jinja2Templates(directory=root / "frontend" / "templates")

# Mount static files from parent directory
app.mount("/static", StaticFiles(directory=root), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML file"""
    html_path = root / "frontend" / "index.html"
    return HTMLResponse(content=html_path.read_text())

@app.get("/health")
async def get_health():
    return JSONResponse(200)

@app.get("/api/{entities}")
async def get_entities(request: Request, entities: str):
    data = {
        "olympiads": ("olimpiadi", ["OlympiadA", "OlympiadB", "OlympiadC", "OlympiadD", "OlympiadE"]),
        "players": ("giocatori", ["PlayerA", "PlayerB", "PlayerC", "PlayerD", "PlayerE"]),
        "events": ("eventi", ["EventA", "EventB", "EventC", "EventD", "EventE"]),
    }
    return templates.TemplateResponse(
        request,
        "entity_list.html",
        {"entity_type": data[entities][0], "items": data[entities][1]}
    )

@app.get("/{filename}.css")
async def serve_css(filename: str):
    """Serve CSS files from frontend directory"""
    css_path = root / "frontend" / f"{filename}.css"
    return Response(content=css_path.read_text(), media_type="text/css")