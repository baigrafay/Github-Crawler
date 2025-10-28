import os
import psycopg2

DATABASE_URL = os.getenv("postgresql://postgres:postgres@localhost:5432/github_crawler")

if not DATABASE_URL:
    print("Please set DATABASE_URL environment variable.")
    exit(1)

try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS repositories (
        id SERIAL PRIMARY KEY,
        repo_id BIGINT UNIQUE,
        name TEXT NOT NULL,
        owner TEXT NOT NULL,
        stars INTEGER DEFAULT 0,
        forks INTEGER DEFAULT 0,
        watchers INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    conn.commit()
    print(" Database and table created successfully.")
    
except Exception as e:
    print(" Error:", e)
finally:
    if conn:
        cur.close()
        conn.close()
