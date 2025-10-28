
import asyncpg
import os
from typing import Dict

class DB:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None

    async def init(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=10)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def upsert_repository(self, repo: Dict):
        """
        repo: dict with keys:
          id, owner, name, full_name, url, description, language, stars, forks, watchers, open_issues_count, created_at, updated_at
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO repositories(repo_id, owner, name, full_name, url, description, language, stars, forks, watchers, open_issues_count, created_at, updated_at, last_crawled_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now())
            ON CONFLICT (repo_id) DO UPDATE
            SET owner = EXCLUDED.owner,
                name = EXCLUDED.name,
                full_name = EXCLUDED.full_name,
                url = EXCLUDED.url,
                description = EXCLUDED.description,
                language = EXCLUDED.language,
                stars = EXCLUDED.stars,
                forks = EXCLUDED.forks,
                watchers = EXCLUDED.watchers,
                open_issues_count = EXCLUDED.open_issues_count,
                updated_at = EXCLUDED.updated_at,
                last_crawled_at = now()
            """, repo['id'], repo['owner'], repo['name'], repo['full_name'], repo['url'], repo.get('description'),
                 repo.get('language'), repo.get('stars', 0), repo.get('forks', 0), repo.get('watchers', 0),
                 repo.get('open_issues_count', 0), repo.get('created_at'), repo.get('updated_at'))

    async def insert_snapshot(self, repo_id, stars, forks, watchers, open_issues_count):
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO repo_snapshots(repo_id, stars, forks, watchers, open_issues_count)
            VALUES($1,$2,$3,$4,$5)
            """, repo_id, stars, forks, watchers, open_issues_count)

    async def save_discovery_seed(self, full_name):
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO discovery_seeds(full_name) VALUES($1) ON CONFLICT (full_name) DO NOTHING
            """, full_name)
