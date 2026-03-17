from itertools import combinations
from collections import defaultdict, deque

import asyncio

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, StreamingResponse

from ..internal import dependencies as dep

router = APIRouter(prefix="/api/events")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def generate_groups_stage(conn, stage_id: int, num_groups: int, participant_ids=None):
    """Tear down and rebuild groups for the given event stage.

    1. Retrieves all enrolled participants from event_participants (or uses participant_ids)
    2. Distributes them across num_groups groups (round-robin)
    3. Creates round-robin matches within each group
    """
    stage = conn.execute(
        "SELECT event_id FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    event_id = stage["event_id"]

    if participant_ids is None:
        participant_rows = conn.execute(
            "SELECT participant_id FROM event_participants WHERE event_id = ? ORDER BY participant_id",
            (event_id,)
        ).fetchall()
        participant_ids = [r["participant_id"] for r in participant_rows]
    n = len(participant_ids)

    # Teardown: CASCADE handles group_participants, matches, match_participants, scores
    conn.execute("DELETE FROM groups WHERE event_stage_id = ?", (stage_id,))

    # TODO: right now in the frontend is a bit confusing that if there are no players enrolled
    #       nothing happens when I try to +1 the number of groups
    if n < 2:
        return

    num_groups = max(1, min(num_groups, n // 2))

    # Distribute into buckets (round-robin)
    buckets = [[] for _ in range(num_groups)]
    for i, pid in enumerate(participant_ids):
        buckets[i % num_groups].append(pid)

    for bucket in buckets:
        group_id = conn.execute(
            "INSERT INTO groups (event_stage_id) VALUES (?) RETURNING id",
            (stage_id,)
        ).fetchone()["id"]

        for seed, pid in enumerate(bucket):
            conn.execute(
                "INSERT INTO group_participants (group_id, participant_id, seed) VALUES (?, ?, ?)",
                (group_id, pid, seed)
            )

        for p1, p2 in combinations(bucket, 2):
            match_id = conn.execute(
                "INSERT INTO matches (group_id) VALUES (?) RETURNING id",
                (group_id,)
            ).fetchone()["id"]
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (match_id, p1)
            )
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (match_id, p2)
            )


def generate_single_elimination_stage(conn, stage_id: int, participant_ids=None):
    """Tear down and rebuild a single-elimination bracket for the given event stage.

    1. Retrieves all enrolled participants from event_participants (or uses participant_ids)
    2. Creates one group containing all participants
    3. Builds the full bracket tree (matches + bracket_matches links)
    4. Assigns participants to first-round matches with standard seeding and byes
    """
    stage = conn.execute(
        "SELECT event_id FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    event_id = stage["event_id"]

    if participant_ids is None:
        participant_rows = conn.execute(
            "SELECT participant_id FROM event_participants WHERE event_id = ? ORDER BY participant_id",
            (event_id,)
        ).fetchall()
        participant_ids = [r["participant_id"] for r in participant_rows]
    n = len(participant_ids)

    # Teardown: CASCADE handles group_participants, matches, match_participants,
    # match_participant_scores, bracket_matches
    conn.execute("DELETE FROM groups WHERE event_stage_id = ?", (stage_id,))

    if n < 2:
        return

    # Next power of 2 >= n
    bracket_size = 1
    while bracket_size < n:
        bracket_size *= 2
    num_rounds = bracket_size.bit_length() - 1

    # Single group for the whole bracket
    group_id = conn.execute(
        "INSERT INTO groups (event_stage_id) VALUES (?) RETURNING id",
        (stage_id,)
    ).fetchone()["id"]

    for seed, pid in enumerate(participant_ids):
        conn.execute(
            "INSERT INTO group_participants (group_id, participant_id, seed) VALUES (?, ?, ?)",
            (group_id, pid, seed)
        )

    # Standard bracket seeding ensures top seeds meet as late as possible.
    # For bracket_size=8: [1,8, 4,5, 2,7, 3,6]
    #   -> match 0: seed 1 vs 8, match 1: seed 4 vs 5, etc.
    def bracket_seeding(size):
        if size == 1:
            return [1]
        half = bracket_seeding(size // 2)
        result = []
        for s in half:
            result.append(s)
            result.append(size + 1 - s)
        return result

    seeds = bracket_seeding(bracket_size)

    # Create matches round by round (first round → final)
    rounds = []
    for round_num in range(num_rounds):
        match_count = bracket_size // (2 ** (round_num + 1))
        round_matches = []
        for _ in range(match_count):
            match_id = conn.execute(
                "INSERT INTO matches (group_id) VALUES (?) RETURNING id",
                (group_id,)
            ).fetchone()["id"]
            round_matches.append(match_id)
        rounds.append(round_matches)

    # Link bracket_matches: each match points to its parent in the next round.
    # Final match has winner_next_match_id = NULL (no next match).
    for round_idx, round_matches in enumerate(rounds):
        for i, match_id in enumerate(round_matches):
            if round_idx == len(rounds) - 1:
                winner_next_match_id = None
            else:
                winner_next_match_id = rounds[round_idx + 1][i // 2]
            conn.execute(
                "INSERT INTO bracket_matches (match_id, winner_next_match_id) VALUES (?, ?)",
                (match_id, winner_next_match_id)
            )

    # Create a third-place match when there are at least 2 rounds (semifinals exist).
    # Losers of the semifinals feed into this match.
    if num_rounds >= 2:
        third_place_match_id = conn.execute(
            "INSERT INTO matches (group_id) VALUES (?) RETURNING id",
            (group_id,)
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO bracket_matches (match_id, winner_next_match_id) VALUES (?, NULL)",
            (third_place_match_id,)
        )
        for semifinal_match_id in rounds[-2]:
            conn.execute(
                "UPDATE bracket_matches SET loser_next_match_id = ? WHERE match_id = ?",
                (third_place_match_id, semifinal_match_id)
            )

    # Assign participants to first-round matches.
    # Seeds beyond n are byes (no participant inserted).
    first_round = rounds[0]
    for i, match_id in enumerate(first_round):
        seed_a = seeds[i * 2]
        seed_b = seeds[i * 2 + 1]
        if seed_a <= n:
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (match_id, participant_ids[seed_a - 1])
            )
        if seed_b <= n:
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (match_id, participant_ids[seed_b - 1])
            )

    # Auto-advance bye participants (matches with only one player) to the next round.
    for match_id in first_round:
        pids = conn.execute(
            "SELECT participant_id FROM match_participants WHERE match_id = ?", (match_id,)
        ).fetchall()
        if len(pids) == 1:
            bm = conn.execute(
                "SELECT winner_next_match_id FROM bracket_matches WHERE match_id = ?", (match_id,)
            ).fetchone()
            if bm and bm["winner_next_match_id"]:
                conn.execute(
                    "INSERT OR IGNORE INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                    (bm["winner_next_match_id"], pids[0]["participant_id"])
                )


def generate_individual_score_stage(conn, stage_id: int, num_groups: int, participant_ids=None):
    """Tear down and rebuild groups for the given individual_score event stage.

    Each group gets exactly one match containing all participants in the group.
    Participants submit individual scores; ranking is by score descending.
    """
    stage = conn.execute(
        "SELECT event_id FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    event_id = stage["event_id"]

    if participant_ids is None:
        participant_rows = conn.execute(
            "SELECT participant_id FROM event_participants WHERE event_id = ? ORDER BY participant_id",
            (event_id,)
        ).fetchall()
        participant_ids = [r["participant_id"] for r in participant_rows]
    n = len(participant_ids)

    conn.execute("DELETE FROM groups WHERE event_stage_id = ?", (stage_id,))

    if n < 1:
        return

    num_groups = max(1, min(num_groups, n))

    buckets = [[] for _ in range(num_groups)]
    for i, pid in enumerate(participant_ids):
        buckets[i % num_groups].append(pid)

    for bucket in buckets:
        if not bucket:
            continue
        group_id = conn.execute(
            "INSERT INTO groups (event_stage_id) VALUES (?) RETURNING id",
            (stage_id,)
        ).fetchone()["id"]

        for seed, pid in enumerate(bucket):
            conn.execute(
                "INSERT INTO group_participants (group_id, participant_id, seed) VALUES (?, ?, ?)",
                (group_id, pid, seed)
            )

        # One match per group; every participant in the group competes in it
        match_id = conn.execute(
            "INSERT INTO matches (group_id) VALUES (?) RETURNING id",
            (group_id,)
        ).fetchone()["id"]
        for pid in bucket:
            conn.execute(
                "INSERT INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (match_id, pid)
            )


def present_individual_score_stage(conn, stage_id: int):
    """Build and render an individual_score stage for display.

    Returns one group entry per group, each with a ranked participant list
    (sorted by score descending; unscored participants go last).
    """
    group_rows = conn.execute(
        "SELECT id FROM groups WHERE event_stage_id = ? ORDER BY id",
        (stage_id,)
    ).fetchall()

    groups = []
    for idx, grow in enumerate(group_rows):
        gid = grow["id"]

        match_row = conn.execute(
            "SELECT id FROM matches WHERE group_id = ? LIMIT 1", (gid,)
        ).fetchone()
        match_id = match_row["id"] if match_row else None

        part_rows = conn.execute(
            "SELECT gp.participant_id, COALESCE(pl.name, t.name) AS display_name, mps.score "
            "FROM group_participants gp "
            "JOIN participants p ON p.id = gp.participant_id "
            "LEFT JOIN players pl ON pl.id = p.player_id "
            "LEFT JOIN teams t ON t.id = p.team_id "
            "LEFT JOIN match_participant_scores mps "
            "  ON mps.match_id = ? AND mps.participant_id = gp.participant_id "
            "WHERE gp.group_id = ? ORDER BY gp.seed",
            (match_id, gid)
        ).fetchall()

        participants = [
            {"id": r["participant_id"], "name": r["display_name"], "score": r["score"]}
            for r in part_rows
        ]
        ranked = sorted(participants, key=lambda p: (p["score"] is None, -(p["score"] or 0)))

        groups.append({
            "name": f"Girone {chr(65 + idx)}",
            "match_id": match_id,
            "participants": ranked,
        })

    total_participants = sum(len(g["participants"]) for g in groups)

    advance_row = conn.execute(
        "SELECT advance_count FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    advance_count = advance_row["advance_count"] if advance_row else None

    stage = {
        "groups": groups,
        "id": stage_id,
        "total_participants": total_participants,
        "advance_count": advance_count,
    }

    return stage


def present_groups_stage(conn, stage_id: int):
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
                scores.setdefault(p1_name, {})[p2_name] = {
                    "match_id": mr["id"],
                    "p1_id": mr["p1_id"],
                    "p2_id": mr["p2_id"],
                    "score": score_str,
                }

        groups.append({
            "name": f"Girone {chr(65 + idx)}",
            "participants": participants,
            "scores": scores,
        })

    total_participants = sum(len(g["participants"]) for g in groups)

    advance_row = conn.execute(
        "SELECT advance_count FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    advance_count = advance_row["advance_count"] if advance_row else None

    stage = {
        "groups": groups,
        "id": stage_id,
        "total_participants": total_participants,
        "advance_count": advance_count,
    }

    return stage


def determine_bracket_winner(p1_id, p1_score, p2_id, p2_score):
    """Return the winner's participant_id, or None if there is no clear winner."""
    if p1_score is None or p2_score is None:
        return None
    if p1_score > p2_score:
        return p1_id
    elif p2_score > p1_score:
        return p2_id
    return None


def _cascade_clear_bracket(conn, match_id, pids):
    """Remove pids from match_id's next match and recurse through the bracket."""
    if not pids:
        return
    bm = conn.execute(
        "SELECT winner_next_match_id FROM bracket_matches WHERE match_id = ?", (match_id,)
    ).fetchone()
    if not bm or bm["winner_next_match_id"] is None:
        return
    next_mid = bm["winner_next_match_id"]
    ph = ",".join("?" * len(pids))
    found = {r["participant_id"] for r in conn.execute(
        f"SELECT participant_id FROM match_participants "
        f"WHERE match_id = ? AND participant_id IN ({ph})",
        [next_mid] + list(pids)
    ).fetchall()}
    if not found:
        return
    for pid in found:
        conn.execute(
            "DELETE FROM match_participants WHERE match_id = ? AND participant_id = ?",
            (next_mid, pid)
        )
    conn.execute("DELETE FROM match_participant_scores WHERE match_id = ?", (next_mid,))
    _cascade_clear_bracket(conn, next_mid, found)


def advance_bracket_winner(conn, match_id, winner_id):
    """Advance winner_id to the next bracket match, cascading removal of old winner.

    If winner_id is None (draw/tie), clears old advancement without inserting anyone.
    """
    bm = conn.execute(
        "SELECT winner_next_match_id FROM bracket_matches WHERE match_id = ?", (match_id,)
    ).fetchone()
    if not bm or bm["winner_next_match_id"] is None:
        return
    next_mid = bm["winner_next_match_id"]
    pids = {r["participant_id"] for r in conn.execute(
        "SELECT participant_id FROM match_participants WHERE match_id = ?", (match_id,)
    ).fetchall()}
    _cascade_clear_bracket(conn, match_id, pids)
    if winner_id is not None:
        conn.execute(
            "INSERT OR IGNORE INTO match_participants (match_id, participant_id) VALUES (?, ?)",
            (next_mid, winner_id)
        )


def advance_bracket_loser(conn, match_id, winner_id):
    """Advance the loser of match_id to the loser_next_match_id (third-place match).

    Removes any previously advanced participant from this match, then inserts the
    new loser. If winner_id is None (draw/no result), clears without inserting.
    """
    bm = conn.execute(
        "SELECT loser_next_match_id FROM bracket_matches WHERE match_id = ?", (match_id,)
    ).fetchone()
    if not bm or bm["loser_next_match_id"] is None:
        return
    loser_next_mid = bm["loser_next_match_id"]
    all_pids = [r["participant_id"] for r in conn.execute(
        "SELECT participant_id FROM match_participants WHERE match_id = ?", (match_id,)
    ).fetchall()]
    # Remove any participant from THIS match already in the third-place match,
    # then clear its scores so they start fresh.
    for pid in all_pids:
        conn.execute(
            "DELETE FROM match_participants WHERE match_id = ? AND participant_id = ?",
            (loser_next_mid, pid)
        )
    conn.execute("DELETE FROM match_participant_scores WHERE match_id = ?", (loser_next_mid,))
    if winner_id is not None:
        loser_id = next((pid for pid in all_pids if pid != winner_id), None)
        if loser_id is not None:
            conn.execute(
                "INSERT OR IGNORE INTO match_participants (match_id, participant_id) VALUES (?, ?)",
                (loser_next_mid, loser_id)
            )


def present_single_elimination_stage(conn, stage_id, view_round):
    """Build a single-elimination stage dict from DB data.

    Returns two consecutive rounds (a parent and its child) for the given
    view_round index, along with navigation metadata.
    """

    rows = conn.execute(
        "SELECT m.id AS match_id, bm.winner_next_match_id, bm.loser_next_match_id "
        "FROM groups g "
        "JOIN matches m ON m.group_id = g.id "
        "JOIN bracket_matches bm ON bm.match_id = m.id "
        "WHERE g.event_stage_id = ?",
        (stage_id,)
    ).fetchall()

    if not rows:
        return {
            "rounds": [],
            "id": stage_id,
            "view_round": 0,
            "total_rounds": 0,
            "has_prev": False,
            "has_next": False,
            "total_rows": 0,
            "third_place_match": None
        }

    match_ids = [r["match_id"] for r in rows]
    placeholders = ",".join("?" * len(match_ids))

    mp_rows = conn.execute(
        f"SELECT mp.match_id, mp.participant_id, "
        f"  COALESCE(pl.name, t.name) AS display_name "
        f"FROM match_participants mp "
        f"JOIN participants p ON p.id = mp.participant_id "
        f"LEFT JOIN players pl ON pl.id = p.player_id "
        f"LEFT JOIN teams t ON t.id = p.team_id "
        f"WHERE mp.match_id IN ({placeholders}) "
        f"ORDER BY mp.participant_id",
        match_ids
    ).fetchall()

    match_parts = defaultdict(list)
    for r in mp_rows:
        match_parts[r["match_id"]].append((r["participant_id"], r["display_name"]))

    score_rows = conn.execute(
        f"SELECT match_id, participant_id, score "
        f"FROM match_participant_scores "
        f"WHERE match_id IN ({placeholders})",
        match_ids
    ).fetchall()
    score_map = {}
    for r in score_rows:
        score_map[(r["match_id"], r["participant_id"])] = r["score"]

    feeders = defaultdict(list)
    matches_by_id = {}

    # Identify the third-place match: it's the loser_next_match_id target of semifinals.
    third_place_id = None
    for r in rows:
        if r["loser_next_match_id"] is not None:
            third_place_id = r["loser_next_match_id"]
            break

    for r in rows:
        if r["match_id"] == third_place_id:
            continue  # exclude third-place match from the main bracket BFS
        matches_by_id[r["match_id"]] = r
        if r["winner_next_match_id"] is not None:
            feeders[r["winner_next_match_id"]].append(r["match_id"])

    final_id = None
    for mid, r in matches_by_id.items():
        if r["winner_next_match_id"] is None:
            final_id = mid
            break

    round_assignment = {}
    bfs_queue = deque([(final_id, 0)])
    while bfs_queue:
        mid, depth = bfs_queue.popleft()
        round_assignment[mid] = depth
        for feeder_id in feeders.get(mid, []):
            bfs_queue.append((feeder_id, depth + 1))

    max_round = max(round_assignment.values()) if round_assignment else 0
    rounds_list = []
    for r in range(max_round, -1, -1):
        mids_in_round = [mid for mid, rn in round_assignment.items() if rn == r]

        match_dicts = []
        for mid in mids_in_round:
            parts = match_parts.get(mid, [])
            p1_id   = parts[0][0] if len(parts) > 0 else None
            p2_id   = parts[1][0] if len(parts) > 1 else None
            p1_name = parts[0][1] if len(parts) > 0 else "?"
            p2_name = parts[1][1] if len(parts) > 1 else "?"
            s1 = score_map.get((mid, p1_id)) if p1_id else None
            s2 = score_map.get((mid, p2_id)) if p2_id else None
            has_score = s1 is not None and s2 is not None
            is_bye    = len(parts) == 1
            clickable = len(parts) == 2
            if is_bye:
                p2_name = "Bye"
            winner_id = None
            if has_score and clickable:
                winner_id = determine_bracket_winner(p1_id, s1, p2_id, s2)
            match_dicts.append({
                "match_id":  mid,
                "p1":        p1_name,
                "p2":        p2_name,
                "p1_id":     p1_id,
                "p2_id":     p2_id,
                "score":     f"{s1} - {s2}" if has_score else None,
                "has_score": has_score,
                "is_bye":    is_bye,
                "clickable": clickable,
                "winner_id": winner_id,
            })

        rounds_list.append({"matches": match_dicts})

    total_rounds = len(rounds_list)

    # Clamp view_round so it always shows 2 rounds when possible
    max_view = max(0, total_rounds - 2)
    view_round = max(0, min(view_round, max_view))

    # Slice: 2 rounds for the current window (1 if only 1 round exists)
    window_end = min(view_round + 2, total_rounds)
    sliced = rounds_list[view_round:window_end]
    for i, round_data in enumerate(sliced):
        round_data["abs_round"] = view_round + i

    # total_rows: parent cards stacked directly, each taking 2 rows.
    # Child cards slot between their two parents with no extra spacing.
    n_first = len(sliced[0]["matches"]) if sliced else 0
    total_rows = n_first * 2

    # Build the third-place match dict; show it only on the last window (no next).
    third_place_match = None
    is_last_window = view_round >= max_view
    if third_place_id is not None and is_last_window:
        parts = match_parts.get(third_place_id, [])
        p1_id   = parts[0][0] if len(parts) > 0 else None
        p2_id   = parts[1][0] if len(parts) > 1 else None
        p1_name = parts[0][1] if len(parts) > 0 else "?"
        p2_name = parts[1][1] if len(parts) > 1 else "?"
        s1 = score_map.get((third_place_id, p1_id)) if p1_id else None
        s2 = score_map.get((third_place_id, p2_id)) if p2_id else None
        has_score = s1 is not None and s2 is not None
        clickable = len(parts) == 2
        tp_winner_id = None
        if has_score and clickable:
            tp_winner_id = determine_bracket_winner(p1_id, s1, p2_id, s2)
        third_place_match = {
            "match_id":  third_place_id,
            "p1":        p1_name,
            "p2":        p2_name,
            "p1_id":     p1_id,
            "p2_id":     p2_id,
            "score":     f"{s1} - {s2}" if has_score else None,
            "has_score": has_score,
            "clickable": clickable,
            "winner_id": tp_winner_id,
        }

    return {
        "rounds": sliced,
        "id": stage_id,
        "view_round": view_round,
        "total_rounds": total_rounds,
        "has_prev": view_round > 0,
        "has_next": view_round < max_view,
        "total_rows": total_rows,
        "third_place_match": third_place_match,
    }


def compute_group_standings(conn, stage_id: int):
    """Compute participant standings within each group, ranked best first.

    For individual_score stages: rank by their single score descending.
    For groups/round_robin stages, rank by wins desc first.
    - outcome: tiebreak by draws desc
    - points:  tiebreak by total points scored desc

    Returns list of {"group_id": int, "ranked_participants": [participant_id, ...]}.
    """
    stage_row = conn.execute(
        "SELECT match_size FROM event_stages WHERE id = ?",
        (stage_id,)
    ).fetchone()
    match_size = stage_row["match_size"] if stage_row else 2

    group_rows = conn.execute(
        "SELECT id FROM groups WHERE event_stage_id = ? ORDER BY id",
        (stage_id,)
    ).fetchall()

    result = []

    if match_size is None:
        for grow in group_rows:
            gid = grow["id"]
            match_row = conn.execute(
                "SELECT id FROM matches WHERE group_id = ? LIMIT 1", (gid,)
            ).fetchone()
            part_rows = conn.execute(
                "SELECT participant_id FROM group_participants WHERE group_id = ? ORDER BY seed",
                (gid,)
            ).fetchall()
            participant_ids = [r["participant_id"] for r in part_rows]
            if not match_row:
                result.append({"group_id": gid, "ranked_participants": participant_ids})
                continue
            score_rows = conn.execute(
                "SELECT participant_id, score FROM match_participant_scores WHERE match_id = ?",
                (match_row["id"],)
            ).fetchall()
            score_map = {r["participant_id"]: r["score"] for r in score_rows}
            ranked = sorted(
                participant_ids,
                key=lambda pid: (score_map.get(pid) is None, -(score_map.get(pid) or 0), pid)
            )
            result.append({"group_id": gid, "ranked_participants": ranked})
        return result

    for grow in group_rows:
        gid = grow["id"]

        part_rows = conn.execute(
            "SELECT participant_id FROM group_participants WHERE group_id = ? ORDER BY seed",
            (gid,)
        ).fetchall()
        participant_ids = [r["participant_id"] for r in part_rows]

        match_rows = conn.execute(
            "SELECT mp1.participant_id AS p1_id, mp2.participant_id AS p2_id, "
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

        stats = {pid: {"wins": 0, "total_points": 0} for pid in participant_ids}
        for mr in match_rows:
            p1, p2 = mr["p1_id"], mr["p2_id"]
            s1, s2 = mr["p1_score"], mr["p2_score"]
            if s1 is None or s2 is None:
                continue
            stats[p1]["total_points"] += s1
            stats[p2]["total_points"] += s2
            if s1 > s2:
                stats[p1]["wins"] += 1
            elif s2 > s1:
                stats[p2]["wins"] += 1

        ranked = sorted(participant_ids,
                        key=lambda pid: (-stats[pid]["wins"], -stats[pid]["total_points"], pid))

        result.append({"group_id": gid, "ranked_participants": ranked})

    return result


def rebuild_subsequent_stages(conn, from_stage_id: int):
    """Tear down and rebuild every stage that follows from_stage_id.

    Called after any score update so that subsequent stages always reflect
    the latest standings. Each stage is rebuilt using populate_next_stage_from_groups,
    which resolves current group standings and regenerates the next stage.
    The cascade stops when there is no further stage or advance_count is 0
    (i.e., the current stage is the final one).
    """
    current_id = from_stage_id
    while True:
        populated = populate_next_stage_from_groups(conn, current_id)
        if not populated:
            break
        row = conn.execute(
            "SELECT event_id, stage_order FROM event_stages WHERE id = ?",
            (current_id,)
        ).fetchone()
        next_row = conn.execute(
            "SELECT id FROM event_stages WHERE event_id = ? AND stage_order = ?",
            (row["event_id"], row["stage_order"] + 1)
        ).fetchone()
        if not next_row:
            break
        current_id = next_row["id"]


def populate_next_stage_from_groups(conn, stage_id: int) -> bool:
    """Populate the stage after stage_id using top advance_count participants from each group.

    Returns True if population was performed, False if nothing to do
    (no advance_count set or no next stage).
    """
    row = conn.execute(
        """
        SELECT es.advance_count,
               (SELECT id                   FROM event_stages WHERE event_id = es.event_id AND stage_order = es.stage_order + 1) AS next_stage_id,
               (SELECT advancement_mechanism FROM event_stages WHERE event_id = es.event_id AND stage_order = es.stage_order + 1) AS next_advancement_mechanism,
               (SELECT match_size           FROM event_stages WHERE event_id = es.event_id AND stage_order = es.stage_order + 1) AS next_match_size
        FROM event_stages es WHERE es.id = ?
        """,
        (stage_id,)
    ).fetchone()

    if not row or not row["advance_count"] or not row["next_stage_id"]:
        return False

    advance_count = row["advance_count"]
    next_stage_id = row["next_stage_id"]
    next_advancement_mechanism = row["next_advancement_mechanism"]
    next_match_size = row["next_match_size"]

    standings = compute_group_standings(conn, stage_id)
    qualified_ids = []
    for group in standings:
        qualified_ids.extend(group["ranked_participants"][:advance_count])

    if not qualified_ids:
        return False

    if next_advancement_mechanism == "bracket":
        generate_single_elimination_stage(conn, next_stage_id, participant_ids=qualified_ids)
    elif next_match_size is None:
        existing_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM groups WHERE event_stage_id = ?", (next_stage_id,)
        ).fetchone()["cnt"]
        num_groups = max(1, existing_count) if existing_count > 0 else 1
        generate_individual_score_stage(conn, next_stage_id, num_groups, participant_ids=qualified_ids)
    else:
        existing_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM groups WHERE event_stage_id = ?", (next_stage_id,)
        ).fetchone()["cnt"]
        num_groups = max(1, existing_count) if existing_count > 0 else 1
        generate_groups_stage(conn, next_stage_id, num_groups, participant_ids=qualified_ids)

    return True


# ---------------------------------------------------------------------------
# List / CRUD
# ---------------------------------------------------------------------------

@router.get("")
def list_events(request: Request):
    conn = request.state.conn
    olympiad_id = dep.get_olympiad_from_request(request)["id"]
    items = conn.execute(
        "SELECT id, name, version FROM events WHERE olympiad_id = ?", (olympiad_id,)
    ).fetchall()
    html_content = dep.render_entity_fragment(
        "entity_list", entities="events", placeholder="Aggiungi un nuovo evento", items=items
    )
    return HTMLResponse(html_content)


@router.post("")
def create_event(request: Request, name: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if dep.check_entity_name_duplication(request, olympiad_id, "events", 0, name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        event = conn.execute(
            """
            INSERT INTO events (name, olympiad_id)
            VALUES (?, ?)
            RETURNING id, name, version, current_stage_order, score_kind
            """,
            (name, olympiad_id)
        ).fetchone()
        event_ctx = _get_event_ctx(conn, event["id"], olympiad_id, event["score_kind"], event["current_stage_order"])
        html_content = dep.render_event_fragment(
            "event_page",
            event_id=event["id"],
            event_name=event["name"],
            event_version=event["version"],
            olympiad_id=olympiad_id,
            tab_id=request.headers.get("X-Tab-Id", ""),
            **event_ctx,
        )

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_olympiad_page(olympiad_id, "event-created", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.get("/{event_id}/edit")
def get_edit_textbox_events(request: Request, event_id: int, name: str = Query(...)):
    ctx = {"entities": "events", "curr_name": name, "id": event_id}
    return dep.templates.TemplateResponse(request, "edit_entity.html", ctx)


@router.get("/{event_id}/cancel-edit")
def cancel_edit_events(request: Request, event_id: int, name: str = Query(...)):
    return dep._cancel_edit(request, "events", event_id, name)


@router.put("/{event_id}")
def rename_events(request: Request, event_id: int, curr_name: str = Form(...), new_name: str = Form(...)):
    conn = request.state.conn
    olympiad_id = dep.get_olympiad_from_request(request)["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if dep.check_entity_name_duplication(request, olympiad_id, "events", 0, new_name):
        result = dep.Status.NAME_DUPLICATION
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        updated_row = conn.execute(
            "UPDATE events SET name = ?, version = version + 1 WHERE id = ? RETURNING id, name, version",
            (new_name, event_id)
        ).fetchone()
        item = {"id": event_id, "name": updated_row["name"], "version": updated_row["version"]}
        html_content = dep.templates.env.get_template("entity_macros.html").module.entity_element(item, "events")

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "event-renamed")
        dep.notify_olympiad_page(olympiad_id, "event-renamed", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.delete("/{event_id}")
def delete_events(request: Request, event_id: int, entity_name: str = Query(..., alias="name")):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        html_content = dep.templates.get_template("entity_delete.html").render()

    html_content += dep._oob_badge_html(request, olympiad_id)
    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "event-deleted")
        dep._event_subscribers.pop(event_id, None)
        dep.notify_olympiad_page(olympiad_id, "event-deleted", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Event page
# ---------------------------------------------------------------------------

@router.get("/{event_id}")
def select_event(request: Request, event_id: int, event_name: str = Query(None, alias="name")):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    event = conn.execute(
        "SELECT id, name, version, current_stage_order, score_kind FROM events WHERE id = ?",
        (event_id,)
    ).fetchone()

    event_ctx = _get_event_ctx(conn, event_id, olympiad_id, event["score_kind"], event["current_stage_order"])

    html_content = dep.render_event_fragment(
        "event_page",
        event_id=event["id"],
        event_name=event["name"],
        event_version=event["version"],
        olympiad_id=olympiad_id,
        is_admin=dep.check_user_authorized(request, olympiad_id),
        tab_id=request.headers.get("X-Tab-Id", ""),
        **event_ctx
    )

    response = HTMLResponse(html_content)

    return response


# ---------------------------------------------------------------------------
# Event status transitions
# ---------------------------------------------------------------------------

@router.post("/{event_id}/start")
def start_event(request: Request, event_id: int):
    conn = request.state.conn

    conn.execute("BEGIN IMMEDIATE")

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, _ = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        first_stage = conn.execute(
            "SELECT id, advancement_mechanism, match_size FROM event_stages WHERE event_id = ? AND stage_order = 1",
            (event_id,)
        ).fetchone()
        if first_stage:
            stage_id   = first_stage["id"]
            stage_kind = dep.STAGE_KIND_MAP[(first_stage["advancement_mechanism"], first_stage["match_size"])]["kind"]
            if stage_kind == "groups":
                groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                generate_groups_stage(conn, stage_id, len(groups))
            elif stage_kind == "individual_score":
                groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                generate_individual_score_stage(conn, stage_id, max(1, len(groups)))
            elif stage_kind == "single_elimination":
                generate_single_elimination_stage(conn, stage_id)
            rebuild_subsequent_stages(conn, stage_id)

    result, response = _set_event_stage_order(request, event_id, new_stage_order=1)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "status-update", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.post("/{event_id}/finish")
def finish_event(request: Request, event_id: int):
    conn = request.state.conn

    max_stage_order = dep.query_get_event_max_stage_order(conn, event_id)

    conn.execute("BEGIN IMMEDIATE")

    # When the event is finished I want to show statistics about the tournament
    # First of all: 1st, 2nd and 3rd place
    # I do not need to go in _set_event_stage_order
    # the only thing I really need from that function is updating in the db teh stage_order to max_stage_order + 1
    # but then is loaded with other meaning that I do not like
    # the only thing I really need is to render the event_page.html with ctx["event_status"] = "finished"
    # so that the right block of the @event_page is rendered

    result, response = _set_event_stage_order(request, event_id, new_stage_order=max_stage_order + 1)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "status-update", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.post("/{event_id}/back-to-registration")
def back_to_registration(request: Request, event_id: int):
    conn = request.state.conn

    conn.execute("BEGIN IMMEDIATE")

    result, response = _set_event_stage_order(request, event_id, new_stage_order=0)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "status-update", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


@router.post("/{event_id}/back-to-running")
def back_to_running(request: Request, event_id: int):
    conn = request.state.conn

    max_stage_order = dep.query_get_event_max_stage_order(conn, event_id)

    conn.execute("BEGIN IMMEDIATE")

    result, response = _set_event_stage_order(request, event_id, new_stage_order=max_stage_order)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "status-update", exclude_tab_id=request.headers.get("X-Tab-Id", ""))
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Participants
# ---------------------------------------------------------------------------

@router.get("/{event_id}/players")
def get_event_players(request: Request, event_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    event_enrolled_participants = dep.query_get_event_enrolled_participants(conn, event_id)
    olympiad_enrolled_participants = dep.query_get_olympiad_enrolled_participants(conn, olympiad_id)

    enrolled_ids = {p["id"] for p in event_enrolled_participants}
    event_available_participants = [p for p in olympiad_enrolled_participants if p["id"] not in enrolled_ids]

    html_content = dep.render_event_fragment(
        "event_player_container",
        event_id=event_id,
        enrolled_participants=event_enrolled_participants,
        available_participants=event_available_participants
    )

    response = HTMLResponse(html_content)

    return response


@router.post("/{event_id}/enroll/{participant_id}")
def enroll_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        conn.execute(
            "INSERT INTO event_participants (event_id, participant_id) VALUES (?, ?)",
            (event_id, participant_id)
        )

        html_content = _render_event_players_section_html(conn, event_id, olympiad_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "enrollment-update")
    else:
        conn.rollback()

    return response


@router.delete("/{event_id}/enroll/{participant_id}")
def unenroll_participant(request: Request, event_id: int, participant_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        conn.execute(
            "DELETE FROM event_participants WHERE event_id = ? AND participant_id = ?",
            (event_id, participant_id)
        )

        html_content = _render_event_players_section_html(conn, event_id, olympiad_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "enrollment-update")
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

@router.get("/{event_id}/stage/{stage_order}")
def get_event_stage(request: Request, event_id: int, stage_order: int):
    conn = request.state.conn

    row = conn.execute(
        """
        SELECT e.current_stage_order, e.version AS event_version, es.id, es.stage_order, es.advancement_mechanism, es.match_size
        FROM events e
        LEFT JOIN event_stages es ON es.event_id = e.id AND es.stage_order = ?
        WHERE e.id = ?
        """,
        (stage_order, event_id)
    ).fetchone()

    stage_id   = row["id"]
    sk = dep.STAGE_KIND_MAP[(row["advancement_mechanism"], row["match_size"])]
    stage_kind = sk["kind"]

    total_stages = conn.execute(
        "SELECT COUNT(*) AS count FROM event_stages WHERE event_id = ?",
        (event_id,)
    ).fetchone()["count"]

    if stage_kind == "groups":
        stage = present_groups_stage(conn, stage_id)
    elif stage_kind == "individual_score":
        stage = present_individual_score_stage(conn, stage_id)
    elif stage_kind == "single_elimination":
        stage = present_single_elimination_stage(conn, stage_id, view_round=0)

    stage["name"] = sk["label"]
    html_content = dep.render_event_fragment(
        "stage_content",
        stage=stage,
        stage_kind=stage_kind,
        stage_order=stage_order,
        total_stages=total_stages,
        event_id=event_id,
        event_version=row["event_version"],
    )

    return HTMLResponse(html_content)


@router.get("/{event_id}/stages-section")
def get_stages_section(request: Request, event_id: int):
    conn = request.state.conn
    return HTMLResponse(_render_stages_section_html(conn, event_id))


@router.post("/{event_id}/stages")
def add_event_stage(request: Request, event_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        advancement_mechanism = "pool"
        match_size = 2

        max_order = dep.query_get_event_max_stage_order(conn, event_id)

        stage_order = max_order + 1
        if max_order > 0:
            conn.execute(
                """
                UPDATE event_stages
                SET advance_count = MAX(2, advance_count)
                WHERE event_id = ? AND stage_order = ?
                """,
                (event_id, max_order)
            )

        stage_id = conn.execute(
            """
            INSERT INTO event_stages (event_id, advancement_mechanism, match_size, stage_order, advance_count)
            VALUES (?, ?, ?, ?, 0)
            RETURNING id
            """,
            (event_id, advancement_mechanism, match_size, stage_order)
        ).fetchone()["id"]

        if stage_order == 1:
            conn.execute("INSERT INTO groups (event_stage_id) VALUES (?)", (stage_id,))

        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@router.delete("/{event_id}/stages/{stage_id}")
def remove_event_stage(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    assert olympiad_id != 0

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        conn.execute("DELETE FROM event_stages WHERE id = ? AND event_id = ?", (stage_id, event_id))

        remaining = conn.execute(
            "SELECT id, advancement_mechanism, match_size FROM event_stages WHERE event_id = ? ORDER BY stage_order",
            (event_id,)
        ).fetchall()
        for i, row in enumerate(remaining):
            conn.execute(
                "UPDATE event_stages SET stage_order = ? WHERE id = ?",
                (i + 1, row["id"])
            )
            if i == 0:
                first_stage_kind = dep.STAGE_KIND_MAP[(row["advancement_mechanism"], row["match_size"])]["kind"]
                stage_id = row["id"]
                if first_stage_kind == "groups":
                    groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                    generate_groups_stage(conn, stage_id, len(groups))
                elif first_stage_kind == "individual_score":
                    groups = conn.execute("SELECT id FROM groups WHERE event_stage_id = ?", (stage_id,)).fetchall()
                    generate_individual_score_stage(conn, stage_id, max(1, len(groups)))
                elif first_stage_kind == "single_elimination":
                    generate_single_elimination_stage(conn, stage_id)

        if remaining:
            conn.execute(
                "UPDATE event_stages SET advance_count = 0 WHERE id = ?",
                (remaining[-1]["id"],)
            )

        conn.execute("UPDATE events SET version = version + 1 WHERE id = ?", (event_id,))

        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@router.post("/{event_id}/stages/{stage_id}/resize")
def resize_stage_groups(request: Request, event_id: int, stage_id: int, num_groups: int = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if result == dep.Status.SUCCESS and not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED
    if result == dep.Status.SUCCESS and not dep.check_min_participants(request, event_id, 2):
        result = dep.Status.NOT_ENOUGH_PARTICIPANTS

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        stage_kind_row = conn.execute(
            "SELECT advancement_mechanism, match_size FROM event_stages WHERE id = ?",
            (stage_id,)
        ).fetchone()

        stage_kind = dep.STAGE_KIND_MAP[(stage_kind_row["advancement_mechanism"], stage_kind_row["match_size"])]["kind"]

        if stage_kind == "individual_score":
            generate_individual_score_stage(conn, stage_id, num_groups)
            stage = present_individual_score_stage(conn, stage_id)
        else:
            generate_groups_stage(conn, stage_id, num_groups)
            stage = present_groups_stage(conn, stage_id)

        new_version = conn.execute(
            "UPDATE events SET version = version + 1 WHERE id = ? RETURNING version",
            (event_id,)
        ).fetchone()["version"]
        html_content = dep.render_event_fragment(
            "stage_groups_content",
            stage=stage,
            event_id=event_id,
            event_version=new_version
        )

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@router.get("/{event_id}/stages/{stage_id}/kind/edit")
def get_edit_stage_kind(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    row = conn.execute(
        "SELECT es.advancement_mechanism, es.match_size "
        "FROM event_stages es "
        "WHERE es.id = ? AND es.event_id = ?",
        (stage_id, event_id)
    ).fetchone()
    return dep.templates.TemplateResponse(
        request,
        "edit_stage_kind.html",
        {
            "stage_id": stage_id,
            "event_id": event_id,
            "current_advancement_mechanism": row["advancement_mechanism"],
            "current_match_size": row["match_size"],
            "stage_kinds": dep.STAGE_KIND_MAP.values(),
        }
    )


@router.get("/{event_id}/stages/{stage_id}/kind/cancel-edit")
def cancel_edit_stage_kind(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    row = conn.execute(
        """
        SELECT es.advancement_mechanism, es.match_size
        FROM event_stages es
        WHERE es.id = ?
        """,
        (stage_id, event_id)
    ).fetchone()
    return dep.templates.TemplateResponse(
        request,
        "stage_kind_display.html",
        {
            "stage_id": stage_id,
            "event_id": event_id,
            "current_label": dep.STAGE_KIND_MAP[(row["advancement_mechanism"], row["match_size"])]["label"],
        }
    )


@router.patch("/{event_id}/stages/{stage_id}")
def update_stage_kind(
    request: Request,
    event_id: int,
    stage_id: int,
    advancement_mechanism: str = Form(...),
    match_size: str = Form(...)
):
    match_size = int(match_size)
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        existing = conn.execute(
            "SELECT stage_order, advance_count FROM event_stages WHERE id = ? AND event_id = ?",
            (stage_id, event_id)
        ).fetchone()
        stage_order = existing["stage_order"]
        advance_count = existing["advance_count"]

        conn.execute("DELETE FROM event_stages WHERE id = ?", (stage_id,))

        new_id = conn.execute(
            """
            INSERT INTO event_stages (event_id, advancement_mechanism, match_size, stage_order, advance_count)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            (event_id, advancement_mechanism, match_size, stage_order, advance_count)
        ).fetchone()["id"]

        derived_kind = dep.STAGE_KIND_MAP[(advancement_mechanism, match_size)]["kind"]
        if stage_order == 1 and derived_kind != "single_elimination":
            conn.execute("INSERT INTO groups (event_stage_id) VALUES (?)", (new_id,))

        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@router.post("/{event_id}/stages/{stage_id}/num-groups")
def set_stage_num_groups(request: Request, event_id: int, stage_id: int, num_groups: int = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        num_groups = max(1, num_groups)
        conn.execute("DELETE FROM groups WHERE event_stage_id = ?", (stage_id,))
        for _ in range(num_groups):
            conn.execute("INSERT INTO groups (event_stage_id) VALUES (?)", (stage_id,))
        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@router.post("/{event_id}/stages/{stage_id}/advance-count")
def set_stage_advance_count(request: Request, event_id: int, stage_id: int, advance_count: int = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        conn.execute(
            "UPDATE event_stages SET advance_count = ? WHERE id = ? AND event_id = ?",
            (max(1, advance_count), stage_id, event_id)
        )
        html_content = _render_stages_section_html(conn, event_id)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "stages-update")
    else:
        conn.rollback()

    return response


@router.get("/{event_id}/stages/{stage_id}/groups-content")
def get_stage_groups_content(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    stage = present_groups_stage(conn, stage_id)
    event_version = conn.execute(
        "SELECT version FROM events WHERE id = ?", (event_id,)
    ).fetchone()["version"]
    return HTMLResponse(dep.render_event_fragment("stage_groups_inner",
        stage=stage, event_id=event_id, event_version=event_version))


@router.get("/{event_id}/stages/{stage_id}/individual-score-content")
def get_stage_individual_score_content(request: Request, event_id: int, stage_id: int):
    conn = request.state.conn
    stage = present_individual_score_stage(conn, stage_id)
    event_version = conn.execute(
        "SELECT version FROM events WHERE id = ?", (event_id,)
    ).fetchone()["version"]
    return HTMLResponse(dep.render_event_fragment(
        "stage_individual_score_inner",
        stage=stage,
        event_id=event_id,
        event_version=event_version,
    ))


@router.get("/{event_id}/stages/{stage_id}/bracket-content")
def get_bracket_content(request: Request, event_id: int, stage_id: int, view_round: int = Query(0)):
    conn = request.state.conn
    stage = present_single_elimination_stage(conn, stage_id, view_round)
    event_version = conn.execute(
        "SELECT version FROM events WHERE id = ?", (event_id,)
    ).fetchone()["version"]
    return HTMLResponse(dep.render_event_fragment(
        "stage_bracket_inner",
        stage=stage,
        event_id=event_id,
        event_version=event_version
    ))


# ---------------------------------------------------------------------------
# Score editing
# ---------------------------------------------------------------------------

@router.get("/{event_id}/score-kind-section")
def get_score_kind_section(request: Request, event_id: int):
    conn = request.state.conn
    row = conn.execute(
        "SELECT score_kind, version FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    return HTMLResponse(dep.render_event_fragment("score_kind_section",
        event_id=event_id, event_version=row["version"],
        score_kinds=dep.SCORE_KINDS,
        current_score_kind=row["score_kind"],
    ))


@router.put("/{event_id}/score_kind")
def update_event_score_kind(request: Request, event_id: int, score_kind: str = Form(...)):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        event_version = conn.execute(
            "UPDATE events SET score_kind = ? WHERE id = ? RETURNING version",
            (score_kind, event_id)
        ).fetchone()["version"]
        html_content = dep.render_event_fragment("score_kind_section",
            event_id=event_id, event_version=event_version,
            score_kinds=dep.SCORE_KINDS,
            current_score_kind=score_kind,
        )
        extra_headers["HX-Retarget"] = "#score-kind-section"
        extra_headers["HX-Reswap"] = "outerHTML"

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "score-kind-update")
    else:
        conn.rollback()

    return response


@router.get("/{event_id}/matches/{match_id}/score/edit")
def get_edit_score(
    request: Request,
    event_id: int,
    match_id: int,
    p1_id: int = Query(...),
    p2_id: int = Query(...),
    view_round: int = Query(0)
):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED
    if result == dep.Status.SUCCESS and not dep.check_previous_stage_complete(request, match_id):
        result = dep.Status.PREVIOUS_STAGE_INCOMPLETE

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        score_rows = conn.execute(
            "SELECT participant_id, score FROM match_participant_scores WHERE match_id = ?",
            (match_id,)
        ).fetchall()
        score_map = {r["participant_id"]: r["score"] for r in score_rows}

        score_kind_row = conn.execute(
            "SELECT e.score_kind FROM matches m "
            "JOIN groups g ON g.id = m.group_id "
            "JOIN event_stages es ON es.id = g.event_stage_id "
            "JOIN events e ON e.id = es.event_id WHERE m.id = ?",
            (match_id,)
        ).fetchone()
        score_kind = score_kind_row["score_kind"] if score_kind_row else "points"

        def get_participant_name(pid):
            row = conn.execute(
                "SELECT COALESCE(pl.name, t.name) AS name FROM participants p "
                "LEFT JOIN players pl ON pl.id = p.player_id "
                "LEFT JOIN teams t ON t.id = p.team_id WHERE p.id = ?", (pid,)
            ).fetchone()
            return row["name"] if row else str(pid)

        template_ctx = {
            "event_id": event_id,
            "match_id": match_id,
            "p1_id": p1_id,
            "p2_id": p2_id,
            "p1_name": get_participant_name(p1_id),
            "p2_name": get_participant_name(p2_id),
            "score_kind": score_kind,
            "p1_score": score_map.get(p1_id),
            "p2_score": score_map.get(p2_id),
            "view_round": view_round,
        }
        html_content = dep.templates.get_template("edit_score.html").render(**template_ctx)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return response


@router.get("/{event_id}/matches/{match_id}/score/cancel-edit")
def cancel_edit_score(
    request: Request,
    event_id: int,
    match_id: int,
    p1_id: int = Query(...),
    p2_id: int = Query(...),
):
    conn = request.state.conn

    score_rows = conn.execute(
        "SELECT participant_id, score FROM match_participant_scores WHERE match_id = ?",
        (match_id,)
    ).fetchall()
    score_map = {r["participant_id"]: r["score"] for r in score_rows}
    p1_score = score_map.get(p1_id)
    p2_score = score_map.get(p2_id)
    score_str = (
        f"{p1_score} - {p2_score}"
        if p1_score is not None and p2_score is not None else None
    )
    ctx = {
        "event_id": event_id,
        "match_id": match_id,
        "p1_id": p1_id,
        "p2_id": p2_id,
        "score": score_str,
    }
    html_content = dep.templates.get_template("score_cell.html").render(**ctx)
    response = HTMLResponse(html_content)

    return response


@router.put("/{event_id}/matches/{match_id}/score")
async def update_match_score(
    request: Request,
    event_id: int,
    match_id: int,
    p1_id: int = Form(...),
    p2_id: int = Form(...),
    score_kind: str = Form("points"),
    p1_score: int = Form(None),
    p2_score: int = Form(None),
    outcome: str = Form(None),
    view_round: int = Form(0)
):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        if score_kind == "outcome":
            if outcome == "p1":
                p1_score, p2_score = 1, 0
            elif outcome == "p2":
                p1_score, p2_score = 0, 1
            else:
                p1_score, p2_score = 0, 0

        for pid, score in [(p1_id, p1_score), (p2_id, p2_score)]:
            conn.execute(
                "INSERT INTO match_participant_scores (match_id, participant_id, score) VALUES (?, ?, ?) "
                "ON CONFLICT (match_id, participant_id) DO UPDATE SET score = excluded.score",
                (match_id, pid, score)
            )

        stage_row = conn.execute(
            "SELECT g.event_stage_id, es.advancement_mechanism FROM matches m "
            "JOIN groups g ON g.id = m.group_id "
            "JOIN event_stages es ON es.id = g.event_stage_id "
            "WHERE m.id = ?",
            (match_id,)
        ).fetchone()
        stage_id = stage_row["event_stage_id"]
        advancement_mechanism = stage_row["advancement_mechanism"]

        if advancement_mechanism == "bracket":
            winner_id = determine_bracket_winner(p1_id, p1_score, p2_id, p2_score)
            advance_bracket_winner(conn, match_id, winner_id)
            advance_bracket_loser(conn, match_id, winner_id)

        rebuild_subsequent_stages(conn, stage_id)

        new_event_version = conn.execute(
            "UPDATE events SET version = version + 1 WHERE id = ? RETURNING version",
            (event_id,)
        ).fetchone()["version"]

        dep.notify_event(event_id, "score-update")

        if advancement_mechanism == "bracket":
            stage = present_single_elimination_stage(conn, stage_id, view_round=view_round)
            html_content = dep.render_event_fragment(
                "stage_bracket_inner",
                stage=stage,
                event_id=event_id,
                event_version=new_event_version
            )
            extra_headers["HX-Retarget"] = "#stage-bracket-inner"
        else:
            stage = present_groups_stage(conn, stage_id)
            html_content = dep.render_event_fragment(
                "stage_groups_inner",
                stage=stage,
                event_id=event_id,
                event_version=new_event_version
            )
            extra_headers["HX-Retarget"] = "#stage-groups-inner"

        extra_headers["HX-Reswap"] = "outerHTML"

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
    else:
        conn.rollback()

    return response


@router.get("/{event_id}/matches/{match_id}/individual-score/edit")
def get_edit_individual_score(
    request: Request,
    event_id: int,
    match_id: int,
    participant_id: int = Query(...),
):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        participant_name = dep.query_get_participant_name(conn, participant_id)
        score = dep.query_get_score(conn, match_id, participant_id)

        ctx = {
            "event_id": event_id,
            "match_id": match_id,
            "participant_id": participant_id,
            "participant_name": participant_name,
            "score": score
        }

        html_content = dep.templates.get_template("edit_individual_score.html").render(**ctx)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)
    return response


@router.put("/{event_id}/matches/{match_id}/individual-score")
async def update_individual_score(
    request: Request,
    event_id: int,
    match_id: int,
    participant_id: int = Form(...),
    score: int = Form(...),
):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    conn.execute("BEGIN IMMEDIATE")

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED
    
    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.NOT_AUTHORIZED:
        html_content = dep.templates.get_template("pin_modal.html").render(olympiad_id=olympiad_id)

    if result == dep.Status.SUCCESS:
        dep.query_update_score(conn, match_id, participant_id, score)
        stage_id = dep.query_get_stage_id_from_match_id(conn, match_id)

        rebuild_subsequent_stages(conn, stage_id)
        stage = present_individual_score_stage(conn, stage_id)

        ctx = {"stage": stage, "event_id": event_id}
        html_content = dep.render_event_fragment("stage_individual_score_inner", **ctx)

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    if result == dep.Status.SUCCESS:
        conn.commit()
        dep.notify_event(event_id, "score-update")
    else:
        conn.rollback()

    return response


# ---------------------------------------------------------------------------
# Notifications / SSE
# ---------------------------------------------------------------------------

@router.get("/{event_id}/title")
def get_event_title(request: Request, event_id: int):
    event = request.state.conn.execute("SELECT name FROM events WHERE id = ?", (event_id,)).fetchone()
    return HTMLResponse(dep.templates.get_template("event_title.html").render(name=event["name"]))


@router.get("/{event_id}/deleted-notice")
def get_event_deleted_notice(request: Request, event_id: int):
    return HTMLResponse(dep.render_modal_fragment("event_deleted"))


@router.get("/{event_id}/olympiad-deleted-notice")
def get_event_olympiad_deleted_notice(request: Request, event_id: int):
    html_content = dep.render_modal_fragment("olympiad_deleted")
    html_content += dep.templates.get_template("olympiad_badge.html").render(olympiad=dep.sentinel_olympiad_badge, oob=True)
    return HTMLResponse(html_content)


@router.get("/{event_id}/olympiad-renamed-notice")
def get_event_olympiad_renamed_notice(request: Request, event_id: int):
    olympiad_data = request.state.conn.execute(
        "SELECT * FROM olympiads JOIN events ON olympiads.id = events.olympiad_id WHERE events.id = ?",
        (event_id,)
    ).fetchone()
    olympiad_id = olympiad_data["id"]
    olympiad_name = olympiad_data["name"]
    olympiad_version = olympiad_data["version"]

    html_content = dep.render_modal_fragment("olympiad_renamed")
    olympiad = {"id": olympiad_id, "name": olympiad_name, "version": olympiad_version}
    html_content += dep.templates.get_template("olympiad_badge.html").render(olympiad=olympiad, oob=True)
    return HTMLResponse(html_content)


@router.get("/{event_id}/sse")
async def event_sse(request: Request, event_id: int, tab_id: str = Query("")):
    queue: asyncio.Queue = asyncio.Queue()
    entry = (tab_id, queue)
    dep._event_subscribers[event_id].add(entry)

    async def generate():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield data
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            dep._event_subscribers[event_id].discard(entry)

    media_type = "text/event-stream"
    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(generate(), media_type=media_type, headers=headers)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_event_ctx(
    conn, event_id: int, olympiad_id: int, score_kind: str, current_stage_order: int
) -> dict:
    ctx = {}

    max_stage_order = dep.query_get_event_max_stage_order(conn, event_id)
    ctx["event_status"] = dep.derive_event_status(current_stage_order, max_stage_order)

    stages_raw = conn.execute(
        "SELECT es.id, es.stage_order, es.advancement_mechanism, es.match_size, es.advance_count, "
        "(SELECT COUNT(*) FROM groups g WHERE g.event_stage_id = es.id) AS num_groups "
        "FROM event_stages es "
        "WHERE es.event_id = ? ORDER BY es.stage_order",
        (event_id,)
    ).fetchall()

    stages = []
    for s in stages_raw:
        stage = dict(s)
        sk = dep.STAGE_KIND_MAP[(s["advancement_mechanism"], s["match_size"])]
        stage["kind"] = sk["kind"]
        stage["label"] = sk["label"]
        stages.append(stage)

    event_enrolled_participants = dep.query_get_event_enrolled_participants(conn, event_id)
    olympiad_enrolled_participants = dep.query_get_olympiad_enrolled_participants(conn, olympiad_id)
    event_enrolled_ids = {p["id"] for p in event_enrolled_participants}
    available_participants = [p for p in olympiad_enrolled_participants if p["id"] not in event_enrolled_ids]

    ctx.update(
        score_kinds=dep.SCORE_KINDS,
        current_score_kind=score_kind,
        stages=stages,
        enrolled_participants=event_enrolled_participants,
        available_participants=available_participants,
    )

    stage_order = min(current_stage_order, max_stage_order)
    row = conn.execute(
        "SELECT es.id, es.stage_order, es.advancement_mechanism, es.match_size "
        "FROM event_stages es "
        "WHERE es.event_id = ? AND es.stage_order = ?",
        (event_id, stage_order)
    ).fetchone()

    if row:
        stage_id = row["id"]
        sk = dep.STAGE_KIND_MAP[(row["advancement_mechanism"], row["match_size"])]
        stage_kind = sk["kind"]
        stage_label = sk["label"]
        total_stages = conn.execute(
            "SELECT COUNT(*) AS count FROM event_stages WHERE event_id = ?",
            (event_id,)
        ).fetchone()["count"]

        if stage_kind == "groups":
            stage = present_groups_stage(conn, stage_id)
        elif stage_kind == "individual_score":
            stage = present_individual_score_stage(conn, stage_id)
        elif stage_kind == "single_elimination":
            stage = present_single_elimination_stage(conn, stage_id, view_round=0)
        else:
            stage = None

        if stage is not None:
            stage["name"] = stage_label
            ctx.update(stage=stage, stage_kind=stage_kind, stage_order=stage_order, total_stages=total_stages)

    return ctx


def _set_event_stage_order(request: Request, event_id: int, new_stage_order: int):
    conn = request.state.conn

    olympiad_badge_ctx = dep.get_olympiad_from_request(request)
    olympiad_id = olympiad_badge_ctx["id"]

    result = dep.Status.SUCCESS
    if not dep.check_user_authorized(request, olympiad_id):
        result = dep.Status.NOT_AUTHORIZED

    html_content, extra_headers = dep._render_operation_denied(result, olympiad_id, "events")

    if result == dep.Status.SUCCESS:
        conn.execute(
            "UPDATE events SET current_stage_order = ? WHERE id = ?",
            (new_stage_order, event_id)
        )

        event = conn.execute(
            "SELECT id, name, version, score_kind FROM events WHERE id = ?",
            (event_id,)
        ).fetchone()

        event_ctx = _get_event_ctx(conn, event_id, olympiad_id, event["score_kind"], new_stage_order)

        html_content = dep.render_event_fragment(
            "event_page",
            event_id=event["id"],
            event_name=event["name"],
            event_version=event["version"],
            olympiad_id=olympiad_id,
            is_admin=dep.check_user_authorized(request, olympiad_id),
            tab_id=request.headers.get("X-Tab-Id", ""),
            **event_ctx
        )

    response = HTMLResponse(html_content)
    response.headers.update(extra_headers)

    return result, response


def _render_stages_section_html(conn, event_id: int):
    stages = dep.query_get_event_stages_with_num_groups(conn, event_id)
    for stage in stages:
        sk = dep.STAGE_KIND_MAP[(stage["advancement_mechanism"], stage["match_size"])]
        stage["kind"] = sk["kind"]
        stage["label"] = sk["label"]

    return dep.render_event_fragment("stages_setup_section", event_id=event_id, stages=stages)


def _render_event_players_section_html(conn, event_id, olympiad_id):
    event_enrolled_participants = dep.query_get_event_enrolled_participants(conn, event_id)
    olympiad_enrolled_participants = dep.query_get_olympiad_enrolled_participants(conn, olympiad_id)

    enrolled_ids = {p["id"] for p in event_enrolled_participants}

    event_available_participants = [p for p in olympiad_enrolled_participants if p["id"] not in enrolled_ids]
    ctx = {
        "event_id": event_id,
        "enrolled_participants": event_enrolled_participants,
        "available_participants": event_available_participants
    }
    return dep.render_event_fragment("event_player_container", **ctx)
