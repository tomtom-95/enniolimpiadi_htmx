-- =====================
-- CORE TABLES
-- =====================
CREATE TABLE olympiads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    pin TEXT NOT NULL CHECK(length(pin) <= 4),
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    olympiad_id INTEGER NOT NULL REFERENCES olympiads(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    current_stage_order INTEGER, -- 0=registration, 1..N=started, >max=finished
    score_kind TEXT NOT NULL CHECK(score_kind IN ('points', 'outcome')),
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (olympiad_id, name)
);

CREATE TABLE players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    olympiad_id INTEGER NOT NULL REFERENCES olympiads(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (olympiad_id, name)
);

CREATE TABLE teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    olympiad_id INTEGER NOT NULL REFERENCES olympiads(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (olympiad_id, name)
);

CREATE TABLE team_players (
    team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
    player_id INTEGER REFERENCES players(id) ON DELETE CASCADE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (team_id, player_id)
);

CREATE TABLE participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER UNIQUE REFERENCES players(id) ON DELETE CASCADE,
    team_id INTEGER UNIQUE REFERENCES teams(id) ON DELETE CASCADE,
    CHECK (
        (player_id IS NULL AND team_id IS NOT NULL) OR
        (player_id IS NOT NULL AND team_id IS NULL)
    )
);

-- =====================
-- TOURNAMENT STRUCTURE
-- =====================
CREATE TABLE stage_kinds (
    kind TEXT PRIMARY KEY,
    label TEXT NOT NULL
);

-- Insert reference data for stage kinds
INSERT INTO stage_kinds (kind, label) VALUES
    ('groups', 'Fase a Gironi'),
    ('round_robin', 'Girone Unico'),
    ('single_elimination', 'Eliminazione Diretta');

CREATE TABLE event_stages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    kind TEXT NOT NULL REFERENCES stage_kinds(kind) CHECK(kind IN ('groups', 'round_robin', 'single_elimination')),
    stage_order INTEGER NOT NULL,
    advance_count INTEGER, -- null for final stage
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (event_id, stage_order)
);

CREATE TABLE groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_stage_id INTEGER NOT NULL REFERENCES event_stages(id) ON DELETE CASCADE,
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE group_participants (
    group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
    participant_id INTEGER REFERENCES participants(id) ON DELETE CASCADE,
    seed INTEGER,
    version INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id, participant_id)
);

-- =====================
-- MATCHES
-- =====================
CREATE TABLE matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'finished')),
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- NOTE: round and position are computed in the application layer
CREATE TABLE bracket_matches (
    match_id INTEGER PRIMARY KEY REFERENCES matches(id) ON DELETE CASCADE,
    next_match_id INTEGER REFERENCES matches(id) ON DELETE SET NULL, -- null for final match
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================
-- MATCH PARTICIPATION
-- =====================
CREATE TABLE match_participants (
    match_id INTEGER REFERENCES matches(id) ON DELETE CASCADE,
    participant_id INTEGER REFERENCES participants(id) ON DELETE CASCADE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, participant_id)
);

CREATE TABLE match_participant_scores (
    match_id INTEGER,
    participant_id INTEGER,
    score INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, participant_id),
    FOREIGN KEY (match_id, participant_id) REFERENCES match_participants(match_id, participant_id) ON DELETE CASCADE
);

-- =====================
-- SESSIONS
-- =====================
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,  -- secure random token
    selected_olympiad_id INTEGER,
    selected_olympiad_version INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
                                                                                                                                                                                                                      
CREATE TABLE session_olympiad_auth (
    session_id TEXT REFERENCES sessions(id) ON DELETE CASCADE,
    olympiad_id INTEGER REFERENCES olympiads(id) ON DELETE CASCADE,
    authorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, olympiad_id)
); 

-- =====================
-- INDEXES
-- =====================
CREATE INDEX idx_events_olympiad_id ON events(olympiad_id);
CREATE INDEX idx_players_olympiad_id ON players(olympiad_id);
CREATE INDEX idx_teams_olympiad_id ON teams(olympiad_id);
CREATE INDEX idx_team_players_player_id ON team_players(player_id);
CREATE INDEX idx_participants_player_id ON participants(player_id);
CREATE INDEX idx_participants_team_id ON participants(team_id);
CREATE INDEX idx_event_stages_event_id ON event_stages(event_id);
CREATE INDEX idx_groups_event_stage_id ON groups(event_stage_id);
CREATE INDEX idx_group_participants_participant_id ON group_participants(participant_id);
CREATE INDEX idx_matches_group_id ON matches(group_id);
CREATE INDEX idx_bracket_matches_next_match_id ON bracket_matches(next_match_id);
CREATE INDEX idx_match_participants_participant_id ON match_participants(participant_id);
