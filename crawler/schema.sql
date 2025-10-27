
CREATE TABLE IF NOT EXISTS repositories (
  repo_id TEXT PRIMARY KEY,             
  owner TEXT NOT NULL,
  name TEXT NOT NULL,
  full_name TEXT NOT NULL UNIQUE,      
  url TEXT,
  description TEXT,
  language TEXT,
  stars BIGINT,                        
  forks BIGINT,
  watchers BIGINT,
  open_issues_count INTEGER,
  created_at TIMESTAMP WITH TIME ZONE,
  updated_at TIMESTAMP WITH TIME ZONE,
  last_crawled_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_repositories_full_name ON repositories(full_name);


CREATE TABLE IF NOT EXISTS repo_snapshots (
  id BIGSERIAL PRIMARY KEY,
  repo_id TEXT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
  snapshot_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  stars BIGINT,
  forks BIGINT,
  watchers BIGINT,
  open_issues_count INTEGER
);

CREATE INDEX IF NOT EXISTS idx_repo_snapshots_repo_time ON repo_snapshots(repo_id, snapshot_time DESC);
