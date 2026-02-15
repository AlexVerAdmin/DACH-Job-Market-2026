import sqlite3
import os

db_path = 'd:/ICH/Practica/data/jobs_database.sqlite'
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT source, url FROM vacancies WHERE (length(description) < 600 OR description = title) AND source = 'stepstone' LIMIT 5")
rows = cursor.fetchall()
urls = [r[1] for r in rows]
with open('urls.txt', 'w') as f:
    f.write('\n'.join(urls))
print(f"Wrote {len(urls)} URLs to urls.txt")
