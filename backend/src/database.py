import sqlite3
from pathlib import Path
from typing import Optional


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Create a new database connection with foreign keys enabled."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path, schema_path: Path):
    """Initialize the database by executing the schema."""
    with open(schema_path, "r") as f:
        schema = f.read()

    conn = get_connection(db_path)
    try:
        conn.executescript(schema)
        conn.commit()
    finally:
        conn.close()


def seed_dummy_data(db_path: Path):
    """Fill the database with dummy olympiads, players, and events."""
    conn = get_connection(db_path)
    try:
        # Insert dummy olympiads
        olympiads = [
            ("OlympiadA", "1234"),
            ("OlympiadB", "2345"),
            ("OlympiadC", "3456"),
            ("OlympiadD", "4567"),
            ("OlympiadE", "5678"),
        ]
        conn.executemany(
            "INSERT INTO olympiads (name, pin) VALUES (?, ?)",
            olympiads
        )

        # Insert dummy players (5 per olympiad)
        players = []
        for olympiad_id in range(1, 6):
            for player_name in ["PlayerA", "PlayerB", "PlayerC", "PlayerD", "PlayerE"]:
                players.append((olympiad_id, f"{player_name}_{olympiad_id}"))
        conn.executemany(
            "INSERT INTO players (olympiad_id, name) VALUES (?, ?)",
            players
        )

        # Insert dummy events (5 per olympiad)
        events = []
        for olympiad_id in range(1, 6):
            for i, event_name in enumerate(["EventA", "EventB", "EventC", "EventD", "EventE"]):
                score_kind = "points" if i % 2 == 0 else "outcome"
                events.append((olympiad_id, f"{event_name}_{olympiad_id}", score_kind))
        conn.executemany(
            "INSERT INTO events (olympiad_id, name, score_kind) VALUES (?, ?, ?)",
            events
        )

        conn.commit()
    finally:
        conn.close()


# def get_olympiads(db_path: Path) -> list[dict]:
#     conn = get_connection(db_path)
#     try:
#         cursor = conn.execute(f"SELECT id, name, version FROM olympiads")
#         return [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
#     finally:
#         conn.close()

# def get_entities(db_path: Path, entity_type: str, olympiad_id: str) -> Optional[list[dict]]:
#     """Retrieve all entities of a given type (olympiads, players, events)."""
#     conn = get_connection(db_path)
#     try:
#         if olympiad_id:
#             cursor = conn.execute(f"SELECT id FROM olympiads WHERE id = {olympiad_id}")
#             row = cursor.fetchone()
#             if row:
#                 cursor = conn.execute(
#                     f"SELECT e.id, e.name, e.version FROM {entity_type} e JOIN olympiads o ON o.id = e.olympiad_id WHERE o.id = {olympiad_id}"
#                 )
#                 return [{"id": row["id"], "name": row["name"], "version": row["version"]} for row in cursor.fetchall()]
#             else:
#                 # olympaid_id does not exist anymore in the olympiads database
#                 return None
#         else:
#             # User might not have selected any olympiad (empty olympiad badge)
#             return None
#     finally:
#         conn.close()


def create_entity(db_path: Path, entity_type: str, name: str) -> dict:
    """Create a new entity and return it with id, name, and version."""
    allowed_tables = {"olympiads", "players", "events"}
    if entity_type not in allowed_tables:
        raise ValueError(f"Invalid entity type: {entity_type}")

    conn = get_connection(db_path)
    try:
        if entity_type == "olympiads":
            cursor = conn.execute(
                "INSERT INTO olympiads (name, pin) VALUES (?, ?) RETURNING id, name, version",
                (name, "0000")
            )
        else:
            raise ValueError(f"Creating {entity_type} without olympiad_id not supported yet")

        row = cursor.fetchone()
        conn.commit()
        return {"id": row["id"], "name": row["name"], "version": row["version"]}
    finally:
        conn.close()


def get_olympiad(db_path: Path, olympiad_id: int) -> dict | None:
    """Retrieve a single olympiad by ID."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT id, name, version FROM olympiads WHERE id = ?",
            (olympiad_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {"id": row["id"], "name": row["name"], "version": row["version"]}
    finally:
        conn.close()