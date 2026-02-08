import sqlite3
from itertools import combinations
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
            ("OlympiadF", "1234"),
            ("OlympiadG", "2345"),
            ("OlympiadH", "3456"),
            ("OlympiadI", "4567"),
            ("OlympiadJ", "4567"),
            ("OlympiadK", "4567"),
            ("OlympiadL", "5678"),
            ("OlympiadM", "5678"),
            ("OlympiadN", "5678"),
            ("OlympiadO", "5678"),
            ("OlympiadP", "5678"),
            ("OlympiadQ", "5678"),
            ("OlympiadR", "5678"),
            ("OlympiadS", "5678"),
            ("OlympiadT", "5678"),
            ("OlympiadU", "5678"),
            ("OlympiadV", "5678"),
            ("OlympiadX", "5678"),
            ("OlympiadY", "5678"),
            ("OlympiadZ", "5678"),
        ]
        conn.executemany("INSERT INTO olympiads (name, pin) VALUES (?, ?)", olympiads)

        olympiad_id = 1

        players = [
            (olympiad_id, "Player1"),
            (olympiad_id, "Player2"),
            (olympiad_id, "Player3"),
            (olympiad_id, "Player4"),
            (olympiad_id, "Player5"),
            (olympiad_id, "Player6"),
            (olympiad_id, "Player7"),
            (olympiad_id, "Player8"),
            (olympiad_id, "Player9"),
            (olympiad_id, "Player10"),
            (olympiad_id, "Player11"),
            (olympiad_id, "Player12"),
            (olympiad_id, "Player13"),
            (olympiad_id, "Player14"),
            (olympiad_id, "Player15"),
            (olympiad_id, "Player16")
        ]
        conn.executemany("INSERT INTO players (olympiad_id, name) VALUES (?, ?)", players)

        events = [
            (olympiad_id, "Event1" , "points"),
            (olympiad_id, "Event2" , "points"),
            (olympiad_id, "Event3" , "points"),
            (olympiad_id, "Event4" , "points"),
            (olympiad_id, "Event5" , "points"),
            (olympiad_id, "Event6" , "points"),
            (olympiad_id, "Event7" , "points"),
            (olympiad_id, "Event8" , "points"),
            (olympiad_id, "Event9" , "points"),
            (olympiad_id, "Event10", "points"),
            (olympiad_id, "Event11", "points"),
            (olympiad_id, "Event12", "points"),
            (olympiad_id, "Event13", "points"),
            (olympiad_id, "Event14", "points"),
            (olympiad_id, "Event15", "points"),
            (olympiad_id, "Event16", "points")
        ]
        conn.executemany("INSERT INTO events (olympiad_id, name, score_kind) VALUES (?, ?, ?)", events)

        # Create event stages
        conn.execute(
            "INSERT INTO event_stages (event_id, kind, stage_order) VALUES (?, ?, ?)",
            (1, "groups", 1)
        )
        conn.execute(
            "INSERT INTO event_stages (event_id, kind, stage_order) VALUES (?, ?, ?)",
            (1, "single_elimination", 2)
        )

        # Create first group of event_stage_id 1
        conn.execute("INSERT INTO groups (event_stage_id) VALUES (?)", (1,))

        # Create second group of event_stage_id 1
        conn.execute("INSERT INTO groups (event_stage_id) VALUES (?)", (1,))

        # Create first and only group of event_stage_id 2
        conn.execute("INSERT INTO groups (event_stage_id) VALUES (?)", (2,))

        # Now I must create participants with team_id = NULL (they are just player)
        participants = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
        ]
        for participant in participants:
            conn.execute("INSERT INTO participants (player_id, team_id) VALUES (?, ?)", (participant, None))

        # Insert the participants into group_participants table
        for i in range(1, 9):
            conn.execute("INSERT INTO group_participants (group_id, participant_id) VALUES (?, ?)", (1, i))
        for i in range(9, 17):
            conn.execute("INSERT INTO group_participants (group_id, participant_id) VALUES (?, ?)", (2, i))

        # Insert matches
        for i in range(28):
            conn.execute("INSERT INTO matches (group_id) VALUES (?)", (1,))
        for i in range(28):
            conn.execute("INSERT INTO matches (group_id) VALUES (?)", (2,))

        # Insert match_participants (round-robin pairings)
        group1_participants = list(range(1, 9))
        group1_pairings = list(combinations(group1_participants, 2))
        for match_idx, (p1, p2) in enumerate(group1_pairings):
            match_id = match_idx + 1
            conn.execute("INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)", (match_id, p1))
            conn.execute("INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)", (match_id, p2))

        group2_participants = list(range(9, 17))
        group2_pairings = list(combinations(group2_participants, 2))
        for match_idx, (p1, p2) in enumerate(group2_pairings):
            match_id = match_idx + 29
            conn.execute("INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)", (match_id, p1))
            conn.execute("INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)", (match_id, p2))

        # Insert match_participant_scores (dummy scores)
        scores = [
            (3, 1), (2, 0), (1, 3), (0, 2), (3, 2), (2, 1), (1, 0),
            (2, 3), (0, 1), (3, 0), (1, 2), (2, 2), (3, 1), (0, 3),
            (1, 1), (2, 0), (3, 2), (0, 1), (2, 3), (1, 0), (3, 1),
            (0, 2), (2, 1), (1, 3), (3, 0), (2, 2), (0, 1), (1, 2),
        ]
        for match_idx, ((p1, p2), (s1, s2)) in enumerate(zip(group1_pairings, scores)):
            match_id = match_idx + 1
            conn.execute("INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?)", (match_id, p1, s1))
            conn.execute("INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?)", (match_id, p2, s2))

        for match_idx, ((p1, p2), (s1, s2)) in enumerate(zip(group2_pairings, scores)):
            match_id = match_idx + 29
            conn.execute("INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?)", (match_id, p1, s1))
            conn.execute("INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?)", (match_id, p2, s2))

        conn.commit()
    finally:
        conn.close()
