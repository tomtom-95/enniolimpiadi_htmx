from itertools import combinations
from collections import defaultdict, deque

def generate_groups_stage(conn, stage_id: int, num_groups: int):
    """Tear down and rebuild groups for the given event stage.

    1. Retrieves all enrolled participants from event_participants
    2. Distributes them across num_groups groups (round-robin)
    3. Creates round-robin matches within each group
    """
    stage = conn.execute(
        "SELECT event_id FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    event_id = stage["event_id"]

    participant_rows = conn.execute(
        "SELECT participant_id FROM event_participants WHERE event_id = ? ORDER BY participant_id",
        (event_id,)
    ).fetchall()
    participant_ids = [r["participant_id"] for r in participant_rows]
    n = len(participant_ids)

    # Teardown: CASCADE handles group_participants, matches, match_participants, scores
    conn.execute("DELETE FROM groups WHERE event_stage_id = ?", (stage_id,))

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


def generate_single_elimination_stage(conn, stage_id: int):
    """Tear down and rebuild a single-elimination bracket for the given event stage.

    1. Retrieves all enrolled participants from event_participants
    2. Creates one group containing all participants
    3. Builds the full bracket tree (matches + bracket_matches links)
    4. Assigns participants to first-round matches with standard seeding and byes
    """
    stage = conn.execute(
        "SELECT event_id FROM event_stages WHERE id = ?", (stage_id,)
    ).fetchone()
    event_id = stage["event_id"]

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
    # Final match has next_match_id = NULL.
    for round_idx, round_matches in enumerate(rounds):
        for i, match_id in enumerate(round_matches):
            if round_idx == len(rounds) - 1:
                next_match_id = None
            else:
                next_match_id = rounds[round_idx + 1][i // 2]
            conn.execute(
                "INSERT INTO bracket_matches (match_id, next_match_id) VALUES (?, ?)",
                (match_id, next_match_id)
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
                scores.setdefault(p1_name, {})[p2_name] = score_str

        groups.append({
            "name": f"Girone {chr(65 + idx)}",
            "participants": participants,
            "scores": scores,
        })

    total_participants = sum(len(g["participants"]) for g in groups)

    stage = {
        "groups": groups,
        "id": stage_id,
        "total_participants": total_participants
    }

    return stage


def present_single_elimination_stage(conn, stage_id):
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
        return { "rounds": [], "id": stage_id }

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

    res = { "rounds": rounds_list, "id": stage_id }

    return res