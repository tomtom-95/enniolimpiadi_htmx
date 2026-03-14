#!/usr/bin/env python3
"""Seed the application with dummy data by calling the HTTP API.

Run this after the app is up:
    python seed.py [base_url]

Default base_url: http://localhost:8000
"""

import re
import sys

import httpx

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


def extract_id(html: str, pattern: str) -> int:
    match = re.search(pattern, html)
    if not match:
        raise ValueError(f"Pattern {pattern!r} not found in:\n{html[:500]}")
    return int(match.group(1))


def extract_all_ids(html: str, pattern: str) -> list[int]:
    return [int(m) for m in re.findall(pattern, html)]


def main():
    with httpx.Client(base_url=BASE_URL) as client:
        # Hit any endpoint to get a session cookie
        client.get("/health")

        # --- OlympiadA ---
        r = client.post("/api/olympiads", data={"name": "OlympiadA", "pin": "1234"})
        r.raise_for_status()
        olympiad_id = extract_id(r.text, r'id="olympiads-(\d+)"')
        print(f"Created OlympiadA id={olympiad_id}")

        headers = {
            "X-Olympiad-Id": str(olympiad_id),
            "X-Olympiad-Name": "OlympiadA",
            "X-Olympiad-Version": "1",
        }

        # --- Players ---
        participant_ids = []
        for i in range(1, 17):
            r = client.post("/api/players", data={"name": f"Player{i}"}, headers=headers)
            r.raise_for_status()
            # Participant IDs are embedded in the enrollment links
            pids = extract_all_ids(
                r.text, rf'/api/events/\d+/enroll/(\d+)'
            )
            # Fallback: use players-N id and assume participant_id equals player_id in fresh DB
            if not pids:
                player_id = extract_id(r.text, r'id="players-(\d+)"')
                participant_ids.append(player_id)
            else:
                participant_ids.extend(pids)
        print(f"Created 16 players")

        # Fetch participant IDs via a temporary event (or use a dedicated request)
        # After creating players, get participant IDs by creating Event1 and reading the players section.

        # --- Events ---
        event_ids = []
        for i in range(1, 17):
            r = client.post("/api/events", data={"name": f"Event{i}"}, headers=headers)
            r.raise_for_status()
            eid = extract_id(r.text, r'sse-connect="/api/events/(\d+)/sse"')
            event_ids.append(eid)
        print(f"Created 16 events")

        event1_id = event_ids[0]
        event2_id = event_ids[1]

        # Get participant IDs from the Event1 players section
        r = client.get(f"/api/events/{event1_id}/players", headers=headers)
        r.raise_for_status()
        participant_ids = extract_all_ids(
            r.text, rf'/api/events/{event1_id}/enroll/(\d+)'
        )
        print(f"Participants: {participant_ids}")

        # --- Event1: pool/groups stage + bracket stage ---

        # Stage 1: pool/2 (default)
        r = client.post(f"/api/events/{event1_id}/stages", headers=headers)
        r.raise_for_status()
        stage1_id = extract_all_ids(
            r.text, rf'hx-delete="/api/events/{event1_id}/stages/(\d+)"'
        )[0]
        print(f"Event1 Stage1 id={stage1_id}")

        # Stage 2: add default pool/2, then PATCH to bracket/2
        r = client.post(f"/api/events/{event1_id}/stages", headers=headers)
        r.raise_for_status()
        all_stage_ids = extract_all_ids(
            r.text, rf'hx-delete="/api/events/{event1_id}/stages/(\d+)"'
        )
        stage2_id = all_stage_ids[-1]

        r = client.patch(
            f"/api/events/{event1_id}/stages/{stage2_id}",
            data={"advancement_mechanism": "bracket", "match_size": "2"},
            headers=headers,
        )
        r.raise_for_status()
        print(f"Event1 Stage2 set to bracket/2")

        # Set stage 1 to 2 groups
        r = client.post(
            f"/api/events/{event1_id}/stages/{stage1_id}/num-groups",
            data={"num_groups": "2"},
            headers=headers,
        )
        r.raise_for_status()

        # Enroll all 16 participants in Event1
        for pid in participant_ids:
            r = client.post(f"/api/events/{event1_id}/enroll/{pid}", headers=headers)
            r.raise_for_status()
        print(f"Enrolled {len(participant_ids)} participants in Event1")

        # --- Event2: individual_score stage ---

        r = client.post(f"/api/events/{event2_id}/stages", headers=headers)
        r.raise_for_status()
        stage3_id = extract_all_ids(
            r.text, rf'hx-delete="/api/events/{event2_id}/stages/(\d+)"'
        )[0]

        # PATCH to pool/0 (individual_score)
        r = client.patch(
            f"/api/events/{event2_id}/stages/{stage3_id}",
            data={"advancement_mechanism": "pool", "match_size": "0"},
            headers=headers,
        )
        r.raise_for_status()
        # stage ID may have changed after PATCH — re-extract
        stage3_id = extract_all_ids(
            r.text, rf'hx-delete="/api/events/{event2_id}/stages/(\d+)"'
        )[0]
        print(f"Event2 Stage id={stage3_id} set to individual_score")

        # Set to 2 groups
        r = client.post(
            f"/api/events/{event2_id}/stages/{stage3_id}/num-groups",
            data={"num_groups": "2"},
            headers=headers,
        )
        r.raise_for_status()

        # Enroll first 8 participants in Event2
        for pid in participant_ids[:8]:
            r = client.post(f"/api/events/{event2_id}/enroll/{pid}", headers=headers)
            r.raise_for_status()
        print(f"Enrolled 8 participants in Event2")

        # --- Remaining olympiads ---
        remaining_olympiads = [
            ("OlympiadB", "2345"), ("OlympiadC", "3456"), ("OlympiadD", "4567"),
            ("OlympiadE", "5678"), ("OlympiadF", "1234"), ("OlympiadG", "2345"),
            ("OlympiadH", "3456"), ("OlympiadI", "4567"), ("OlympiadJ", "4567"),
            ("OlympiadK", "4567"), ("OlympiadL", "5678"), ("OlympiadM", "5678"),
            ("OlympiadN", "5678"), ("OlympiadO", "5678"), ("OlympiadP", "5678"),
            ("OlympiadQ", "5678"), ("OlympiadR", "5678"), ("OlympiadS", "5678"),
            ("OlympiadT", "5678"), ("OlympiadU", "5678"), ("OlympiadV", "5678"),
            ("OlympiadX", "5678"), ("OlympiadY", "5678"), ("OlympiadZ", "5678"),
        ]
        for name, pin in remaining_olympiads:
            r = client.post("/api/olympiads", data={"name": name, "pin": pin})
            r.raise_for_status()
        print(f"Created {len(remaining_olympiads)} additional olympiads")

        print("Seed complete!")


if __name__ == "__main__":
    main()
