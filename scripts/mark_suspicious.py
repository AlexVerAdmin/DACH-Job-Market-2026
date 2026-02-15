import sqlite3
import configparser
import os

def mark_suspicious():
    # 1. Load config
    config = configparser.ConfigParser()
    config.read('settings.ini', encoding='utf-8')
    
    db_path = config.get('Database', 'path', fallback='data/jobs_database.sqlite')
    exclude_keywords = [k.strip().lower() for k in config.get('Scraping', 'exclude_keywords', fallback='').split(',') if k.strip()]
    
    # Define level patterns for cross-check
    levels = {section: [v.strip().lower() for v in values.split(',')] for section, values in config.items('Levels')}
    
    if not os.path.exists(db_path):
        print(f"[!] Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"[*] Analyzing vacancies in {db_path}...")

    # Reset markers first (optional, but good for re-runs)
    cursor.execute("UPDATE vacancies SET is_suspicious = 0")
    
    # 2. Mark by EXCLUDE_KEYWORDS
    for kw in exclude_keywords:
        cursor.execute("UPDATE vacancies SET is_suspicious = 1 WHERE LOWER(title) LIKE ?", (f'%{kw}%',))
        if cursor.rowcount > 0:
            print(f"  [+] Marked {cursor.rowcount} records containing '{kw}'")

    # 3. Mark by Level Mismatch (e.g. search_level is Junior, but title says Senior)
    cursor.execute("SELECT internal_id, title, search_level FROM vacancies WHERE is_suspicious = 0 AND title IS NOT NULL")
    rows = cursor.fetchall()
    
    mismatch_count = 0
    for idx, title, current_level in rows:
        title_lower = title.lower()
        
        # Check if title suggests a DIFFERENT level than assigned
        suggested_level = None
        for level_name, syns in levels.items():
            if any(s in title_lower for s in syns if s):
                suggested_level = level_name
                break
        
        # If title clearly says "Senior"/"Lead" but it's marked as "Junior" or "General"
        if suggested_level and current_level != suggested_level:
            # Special logic for flagging
            if (current_level == 'Junior' and suggested_level == 'Senior') or \
               (current_level == 'Junior' and suggested_level == 'General' and any(x in title_lower for x in ['manager', 'lead', 'head'])) or \
               (current_level == 'Intern' and suggested_level == 'Senior'):
                cursor.execute("UPDATE vacancies SET is_suspicious = 1 WHERE internal_id = ?", (idx,))
                mismatch_count += 1

    print(f"  [+] Marked {mismatch_count} records due to level mismatch (e.g. Senior in Junior search)")

    conn.commit()
    
    # Summary
    cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_suspicious = 1")
    total = cursor.fetchone()[0]
    print(f"\n[DONE] Total suspicious records marked: {total}")
    print("[TIP] You can now check them in SQLite or your dashboard: SELECT * FROM vacancies WHERE is_suspicious = 1")
    
    conn.close()

if __name__ == "__main__":
    mark_suspicious()
