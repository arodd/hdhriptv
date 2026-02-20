CREATE INDEX IF NOT EXISTS idx_job_runs_started_run_id_desc
  ON job_runs(started_at DESC, run_id DESC);

CREATE INDEX IF NOT EXISTS idx_job_runs_name_started_run_id_desc
  ON job_runs(job_name, started_at DESC, run_id DESC);
