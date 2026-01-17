import sqlite3
from pathlib import Path

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