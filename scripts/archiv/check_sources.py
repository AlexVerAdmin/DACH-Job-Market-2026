import sqlite3

db_path = 'd:/ICH/Practica/data/jobs_database.sqlite'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT source, count(*) FROM vacancies WHERE length(description) < 600 OR description = title GROUP BY source")
rows = cursor.fetchall()
for r in rows:
    print(f"{r[0]}: {r[1]}")
conn.close()
