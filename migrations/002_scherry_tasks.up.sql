CREATE TABLE scherry_tasks (
    id UUID NOT NULL,
    job_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    attempt INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    sequence_number INTEGER NOT NULL DEFAULT 0,
    distribution_key TEXT,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE scherry_tasks_default PARTITION OF scherry_tasks DEFAULT;

-- Lookups and updates by task id (partitioned PK is id, created_at).
CREATE INDEX idx_scherry_tasks_id ON scherry_tasks (id);

-- FindTasksByJobIDAndStatuses, status-filtered bulk updates, ListTasksForJob with
-- status + ORDER BY sequence_number, and partial-retry batch paging.
CREATE INDEX idx_scherry_tasks_job_id_status_seq ON scherry_tasks (job_id, status, sequence_number);

-- Full-retry streaming batch (no status filter): ORDER BY sequence_number.
CREATE INDEX idx_scherry_tasks_job_id_seq ON scherry_tasks (job_id, sequence_number);

-- Analytics time-series range scans over created_at.
CREATE INDEX idx_scherry_tasks_created_at ON scherry_tasks (created_at DESC);
