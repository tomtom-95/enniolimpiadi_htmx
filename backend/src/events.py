from itertools import combinations
from collections import defaultdict, deque

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

    return {
        "groups": groups,
        "id": stage_id,
        "total_participants": total_participants,
        "advance_count": advance_count,
    }


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


def present_single_elimination_stage(conn, stage_id, view_round=0):
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
        return {"rounds": [], "id": stage_id,
                "view_round": 0, "total_rounds": 0, "has_prev": False, "has_next": False,
                "total_rows": 0, "third_place_match": None}

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


def cascade_rebuild_subsequent_stages(conn, from_stage_id: int):
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