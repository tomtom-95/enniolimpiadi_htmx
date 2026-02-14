from itertools import combinations

def construct_groups_stage(conn, stage_id: int, num_groups: int):
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


def construct_single_elimination_stage(conn, stage_id: int):
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
    #   → match 0: seed 1 vs 8, match 1: seed 4 vs 5, etc.
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
