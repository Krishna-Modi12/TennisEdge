CREATE TABLE IF NOT EXISTS users (
    telegram_id BIGINT PRIMARY KEY,
    username TEXT,
    credits INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    credits_added INTEGER,
    payment_amount NUMERIC,
    payment_id TEXT,
    package TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    match_id TEXT NOT NULL,
    market TEXT,
    selection TEXT,
    odds NUMERIC,
    model_prob NUMERIC,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signal_deliveries (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER,
    user_id BIGINT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signal_results (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER,
    actual_winner TEXT,
    is_correct BOOLEAN,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS player_aliases (
    alias TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS player_elo (
    player_name TEXT PRIMARY KEY,
    surface TEXT,
    elo_rating NUMERIC,
    matches_played INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS player_stats (
    player_name TEXT PRIMARY KEY,
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS elo_history (
    id SERIAL PRIMARY KEY,
    event_key TEXT,
    event_date TIMESTAMP,
    winner TEXT,
    loser TEXT,
    surface TEXT,
    tournament TEXT,
    source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS unmatched_players (
    name TEXT PRIMARY KEY,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS sent_signals (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER,
    user_id BIGINT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signal_performance (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER,
    status TEXT,
    pnl NUMERIC,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
