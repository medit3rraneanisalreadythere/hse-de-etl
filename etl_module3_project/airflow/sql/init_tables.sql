CREATE TABLE IF NOT EXISTS staging_user_sessions (
    session_id VARCHAR PRIMARY KEY,
    user_id VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited TEXT,
    device VARCHAR,
    actions TEXT
);

CREATE TABLE IF NOT EXISTS staging_support_tickets (
    ticket_id VARCHAR PRIMARY KEY,
    user_id VARCHAR,
    status VARCHAR,
    issue_type VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    resolution_hours FLOAT
);

CREATE INDEX IF NOT EXISTS idx_sessions_user ON staging_user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status ON staging_support_tickets(status);