import sqlite3
import os
from datetime import datetime
from data_utils import get_job_signature

class DatabaseManager:
    def __init__(self, db_path="data/jobs_database.sqlite"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS vacancies (
                    internal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signature TEXT UNIQUE,
                    api_id TEXT,
                    title TEXT,
                    company TEXT,
                    location TEXT,
                    country_api TEXT,
                    salary_min REAL,
                    salary_max REAL,
                    salary_is_predicted INTEGER DEFAULT 0,
                    description TEXT,
                    created TEXT,
                    url TEXT,
                    search_query TEXT,
                    search_level TEXT,
                    first_seen TEXT,
                    last_seen TEXT,
                    source TEXT,
                    translated_title TEXT
                )
            ''')
            
            # Check if translated_title column exists (for existing DBs)
            cursor.execute("PRAGMA table_info(vacancies)")
            columns = [info[1] for info in cursor.fetchall()]
            if 'translated_title' not in columns:
                cursor.execute("ALTER TABLE vacancies ADD COLUMN translated_title TEXT")
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS salary_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country TEXT,
                    role TEXT,
                    month TEXT,
                    avg_salary REAL,
                    UNIQUE(country, role, month)
                )
            ''')
            conn.commit()

    def save_vacancies(self, jobs):
        if not jobs:
            return 0
        
        today = datetime.now().strftime("%Y-%m-%d")
        new_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for job in jobs:
                try:
                    title = job.get('title')
                    # Company/Location might be dicts (from Adzuna) or strings (from others)
                    company = job.get('company', {}).get('display_name') if isinstance(job.get('company'), dict) else job.get('company')
                    location = job.get('location', {}).get('display_name') if isinstance(job.get('location'), dict) else job.get('location')
                    
                    signature = get_job_signature(title, company, location)

                    # Check existence for counting
                    cursor.execute("SELECT source FROM vacancies WHERE signature = ?", (signature,))
                    exists = cursor.fetchone()

                    cursor.execute('''
                        INSERT INTO vacancies (signature, api_id, title, company, location, country_api, 
                                              salary_min, salary_max, salary_is_predicted, description, created, 
                                              url, search_query, search_level, first_seen, last_seen, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(signature) DO UPDATE SET 
                            last_seen = excluded.last_seen,
                            url = COALESCE(excluded.url, vacancies.url),
                            salary_min = CASE 
                                WHEN excluded.salary_min IS NOT NULL AND (vacancies.salary_min IS NULL OR excluded.source != 'adzuna' OR vacancies.source = 'adzuna') 
                                THEN excluded.salary_min 
                                ELSE vacancies.salary_min 
                            END,
                            salary_max = CASE 
                                WHEN excluded.salary_max IS NOT NULL AND (vacancies.salary_max IS NULL OR excluded.source != 'adzuna' OR vacancies.source = 'adzuna') 
                                THEN excluded.salary_max 
                                ELSE vacancies.salary_max 
                            END,
                            salary_is_predicted = CASE 
                                WHEN excluded.salary_min IS NOT NULL AND (vacancies.salary_min IS NULL OR excluded.source != 'adzuna' OR vacancies.source = 'adzuna')
                                THEN excluded.salary_is_predicted 
                                ELSE vacancies.salary_is_predicted 
                            END,
                            description = CASE
                                WHEN length(excluded.description) > length(vacancies.description) THEN excluded.description
                                ELSE vacancies.description
                            END,
                            source = CASE 
                                WHEN excluded.source != 'adzuna' OR vacancies.source = 'adzuna' THEN excluded.source 
                                ELSE vacancies.source 
                            END
                    ''', (
                        signature,
                        str(job.get('api_id') or job.get('id')),
                        title,
                        company,
                        location,
                        job.get('country_search') or job.get('country_api', 'DE'),
                        job.get('salary_min'),
                        job.get('salary_max'),
                        1 if job.get('salary_is_predicted') else 0,
                        job.get('description', ''),
                        job.get('created'),
                        job.get('url') or job.get('redirect_url'),
                        job.get('search_query'),
                        job.get('search_level', 'General'),
                        today,
                        today,
                        job.get('source', 'unknown')
                    ))
                    if not exists:
                        new_count += 1
                except Exception as e:
                    print(f"  [!] Database error for {job.get('title')}: {e}")
            conn.commit()
            
        return new_count

    def get_all_vacancies(self):
        import pandas as pd
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query("SELECT * FROM vacancies", conn)
    def save_salary_history(self, country, role, history_data):
        if not history_data:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for month, avg_salary in history_data.items():
                try:
                    cursor.execute('''
                        INSERT INTO salary_history (country, role, month, avg_salary)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(country, role, month) DO UPDATE SET
                            avg_salary = excluded.avg_salary
                    ''', (country.upper(), role, month, avg_salary))
                except Exception as e:
                    print(f"  [!] History DB error: {e}")
            conn.commit()

    def get_salary_history(self, country=None, role=None):
        import pandas as pd
        query = "SELECT * FROM salary_history WHERE 1=1"
        params = []
        if country:
            query += " AND country = ?"
            params.append(country.upper())
        if role:
            query += " AND role = ?"
            params.append(role)
        
        query += " ORDER BY month ASC"
        
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=params)

    def clear_all_data(self):
        """Полная очистка всех таблиц в базе данных."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vacancies")
            cursor.execute("DELETE FROM salary_history")
            # Сбрасываем автоинкремент
            cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('vacancies', 'salary_history')")
            conn.commit()
            print("[!] Database cleared: vacancies and salary_history tables are now empty.")
