import sqlite3

db_path = 'd:/ICH/Practica/data/jobs_database.sqlite'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("--- XING URLs ---")
cursor.execute("SELECT url FROM vacancies WHERE source = 'xing' LIMIT 3")
for r in cursor.fetchall():
    print(r[0])

print("\n--- AA URLs ---")
cursor.execute("SELECT url FROM vacancies WHERE source = 'arbeitsagentur' LIMIT 3")
for r in cursor.fetchall():
    print(r[0])

conn.close()
