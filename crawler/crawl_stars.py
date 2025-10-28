
import os
import asyncio
import aiohttp
from graphql_client import GraphQLClient
from db import DB
import argparse
import datetime
from dateutil import parser as dateparser


SEARCH_QUERY = """
query($q: String!, $cursor: String) {
  search(query: $q, type: REPOSITORY, first: 50, after: $cursor) {
    repositoryCount
    pageInfo { hasNextPage, endCursor }
    nodes {
      ... on Repository {
        id
        name
        url
        description
        primaryLanguage { name }
        owner { login }
        stargazerCount
        forkCount
        watchers { totalCount }
        createdAt
        updatedAt
      }
    }
  }
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
}
"""

def generate_date_windows(start_date, end_date, step_days=7):
    cur = start_date
    while cur < end_date:
        nxt = cur + datetime.timedelta(days=step_days)
        yield cur.date(), (nxt - datetime.timedelta(days=1)).date()
        cur = nxt

def node_to_repo(node):
    return {
        "id": node.get("id"),
        "owner": node.get("owner", {}).get("login"),
        "name": node.get("name"),
        "full_name": f"{node.get('owner', {}).get('login')}/{node.get('name')}",
        "url": node.get("url"),
        "description": node.get("description"),
        "language": node.get("primaryLanguage", {}).get("name") if node.get("primaryLanguage") else None,
        "stars": node.get("stargazerCount"),
        "forks": node.get("forkCount"),
        "watchers": node.get("watchers", {}).get("totalCount") if node.get("watchers") else 0,
        "open_issues_count": None,
        "created_at": node.get("createdAt"),
        "updated_at": node.get("updatedAt")
    }

async def discover_repos(client: GraphQLClient, target=100000):
    """
    Partition by created date windows (7-day windows) until we collect `target` unique repos.
    Returns list of full_name strings.
    """
    collected = set()
    start_date = datetime.datetime(2010, 1, 1)
    end_date = datetime.datetime.now()
    windows = list(generate_date_windows(start_date, end_date, step_days=7))

    for a, b in windows:
        if len(collected) >= target:
            break
        q = f"created:{a}..{b} sort:updated"
        cursor = None
        while True:
            variables = {"q": q, "cursor": cursor}
            data = await client.execute(SEARCH_QUERY, variables)
            search = data["search"]
            nodes = search.get("nodes", [])
            for node in nodes:
                if not node:
                    continue
                owner = node.get("owner", {}).get("login")
                name = node.get("name")
                if owner and name:
                    full = f"{owner}/{name}"
                    collected.add(full)
                    if len(collected) >= target:
                        break
            if len(collected) >= target:
                break
            pageInfo = search.get("pageInfo", {})
            if not pageInfo.get("hasNextPage"):
                break
            cursor = pageInfo.get("endCursor")
        
        await asyncio.sleep(0.2)
    return list(collected)[:target]

async def fetch_and_persist(full_name, client: GraphQLClient, db: DB, sem: asyncio.Semaphore):
    owner, name = full_name.split("/", 1)
    q = """
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        id
        name
        url
        description
        primaryLanguage { name }
        owner { login }
        stargazerCount
        forkCount
        watchers { totalCount }
        createdAt
        updatedAt
      }
      rateLimit { limit cost remaining resetAt }
    }
    """
    async with sem:
        try:
            data = await client.execute(q, {"owner": owner, "name": name})
            node = data.get("repository")
            if not node:
                return
            repo = node_to_repo(node)
            await db.upsert_repository(repo)
            await db.insert_snapshot(repo['id'], repo['stars'], repo['forks'], repo['watchers'], repo['open_issues_count'])
        except Exception as e:
            print(f"[error] {full_name}: {e}")

async def main_loop(target, dsn, token, concurrency=8):
    async with aiohttp.ClientSession() as session:
        client = GraphQLClient(token, session, min_remaining=50)
        db = DB(dsn)
        await db.init()

        
        print("[discover] starting discovery")
        repo_list = await discover_repos(client, target=target)
        print(f"[discover] discovered {len(repo_list)} repos (target {target})")

        sem = asyncio.Semaphore(concurrency)
        tasks = []
        for full in repo_list:
            tasks.append(asyncio.create_task(fetch_and_persist(full, client, db, sem)))
        
        for i in range(0, len(tasks), 2000):
            batch = tasks[i:i+2000]
            await asyncio.gather(*batch)
            print(f"[progress] finished batch {i}..{i+len(batch)}")
        await db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=int, default=1000, help="Number of repos to crawl")
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--dsn", type=str, default=os.getenv("DATABASE_URL"))
    parser.add_argument("--token", type=str, default=os.getenv("GITHUB_TOKEN"))
    args = parser.parse_args()
    if not args.dsn or not args.token:
        print("Please set DATABASE_URL and GITHUB_TOKEN environment variables or pass via args.")
        exit(1)
    asyncio.run(main_loop(args.target, args.dsn, args.token, concurrency=args.concurrency))
