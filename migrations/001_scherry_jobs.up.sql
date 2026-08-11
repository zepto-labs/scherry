CREATE TABLE scherry_jobs (
    id UUID NOT NULL,
    parent_job_id UUID,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    max_retries INTEGER NOT NULL DEFAULT 3,
    unique_reference_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE scherry_jobs_default PARTITION OF scherry_jobs DEFAULT;

-- Lookups and updates by job id (partitioned PK is id, created_at).
CREATE INDEX idx_scherry_jobs_id ON scherry_jobs (id);

-- CancelPreviousJobs and FindCompletableJobIDs: WHERE name = ? AND status ...
-- Leftmost prefix also serves name-only filters.
CREATE INDEX idx_scherry_jobs_name_status ON scherry_jobs (name, status);

-- Console job list and analytics when filtering by status alone.
CREATE INDEX idx_scherry_jobs_status ON scherry_jobs (status);

-- Retry lineage: FindPendingJobByParentID and jobLineage.
CREATE INDEX idx_scherry_parent_job_id ON scherry_jobs (parent_job_id);

-- Idempotent job submission: WHERE unique_reference_id = ?
CREATE INDEX idx_scherry_jobs_unique_reference_id ON scherry_jobs (unique_reference_id);

-- Console job list ordering (newest-first).
CREATE INDEX idx_scherry_jobs_created_at ON scherry_jobs (created_at DESC);
