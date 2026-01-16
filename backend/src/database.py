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