import sqlite3
import re
import pandas as pd
from collections import Counter

class SkillExtractor:
    def __init__(self, db_path="data/jobs_database.sqlite"):
        self.db_path = db_path
        self.skills_patterns = {
            'Python': r'python',
            'SQL': r'sql',
            'R': r'\br\b',
            'Power BI': r'power\s?bi',
            'Tableau': r'tableau',
            'Excel': r'excel',
            'Machine Learning': r'machine\s?learning|ml',
            'Deep Learning': r'deep\s?learning',
            'Azure': r'azure',
            'AWS': r'aws|amazon\s?web',
            'GCP': r'gcp|google\s?cloud',
            'SAP': r'\bsap\b',
            'Airflow': r'airflow',
            'Spark': r'spark',
            'Pandas': r'pandas',
            'MLOps': r'mlops',
            'Docker': r'docker',
            'Kubernetes': r'kubernetes|k8s',
            'PyTorch': r'pytorch',
            'TensorFlow': r'tensorflow',
            'English': r'english|englisch',
            'German': r'german|deutsch',
            'ETL': r'\betl\b',
            'NoSQL': r'nosql|mongodb|cassandra',
            'CI/CD': r'ci/cd|jenkins|gitlab\s?ci',
            'Git': r'\bgit\b|github|gitlab',
            'Communication': r'communication|kommunikations',
            'Problem Solving': r'problem\s?solving'
        }

    def analyze_skills(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT signature, title, description, source FROM vacancies", conn)
        conn.close()

        # Стоп-слова для "Discovery" режима
        blacklist = {'THE', 'AND', 'FOR', 'WITH', 'FROM', 'THIS', 'THAT', 'YOUR', 'WILL', 'TEAM', 'DATA', 'WORK'}
        # Немецкие стоп-слова (частые сущности в вакансиях)
        blacklist.update({'WIR', 'SIE', 'UND', 'MIT', 'DAS', 'DIESE', 'EINE', 'ODER', 'SIND', 'IHRE'})
        # Мета-слова
        blacklist.update({'HOME', 'OFFICE', 'JOB', 'TASKS', 'PROFILE', 'OFFER', 'BENEFITS'})

        print(f"[Skills] Анализ {len(df)} вакансий (Discovery mode ON)...")
        
        results = []
        for idx, row in df.iterrows():
            orig_text = f"{row['title']} {row['description']}"
            lower_text = orig_text.lower()
            found_skills = set()
            
            # 1. Поиск установленных навыков (Case-insensitive)
            for skill, pattern in self.skills_patterns.items():
                if re.search(pattern, lower_text):
                    found_skills.add(skill)
            
            # 2. Discovery: Поиск неизвестных технических терминов
            # Ищем: 1) ВСЕ_ЗАГЛАВНЫЕ(3+), 2) CamelCase(starts Upper) 3) Спец-символы (.js, /)
            candidates = re.findall(r'\b[A-Z]{3,}\b|\b[A-Z][a-z]+[A-Z][A-Za-z0-9]+\b|\b[A-Za-z]+[./][A-Za-z]{2,}\b', orig_text)
            
            for cond in candidates:
                # Очистка и проверка
                cond_clean = cond.strip('.,/()')
                if len(cond_clean) < 2: continue
                
                # Исключаем стоп-слова
                if cond_clean.upper() in blacklist: continue
                
                # Исключаем то, что уже нашли через patterns
                is_known = False
                for known in found_skills:
                    if cond_clean.lower() in known.lower() or known.lower() in cond_clean.lower():
                        is_known = True
                        break
                
                if not is_known:
                    found_skills.add(cond_clean)
            
            results.append({
                'signature': row['signature'],
                'skills': ", ".join(sorted(list(found_skills)))
            })
        
        # Обновляем таблицу вакансий
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Проверяем наличие колонки
            cursor.execute("PRAGMA table_info(vacancies)")
            columns = [c[1] for c in cursor.fetchall()]
            if 'extracted_skills' not in columns:
                cursor.execute("ALTER TABLE vacancies ADD COLUMN extracted_skills TEXT")
            
            for res in results:
                cursor.execute("UPDATE vacancies SET extracted_skills = ? WHERE signature = ?", 
                             (res['skills'], res['signature']))
            conn.commit()
            conn.close()
            print(f"[Skills] Навыки извлечены и сохранены в БД.")
        except Exception as e:
            print(f"[!] Ошибка при сохранении навыков: {e}")

    def get_top_skills(self, min_salary=None):
        conn = sqlite3.connect(self.db_path)
        query = "SELECT extracted_skills FROM vacancies WHERE extracted_skills IS NOT NULL"
        if min_salary:
            query += f" AND (salary_min >= {min_salary} OR salary_max >= {min_salary})"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        all_skills = []
        for s in df['extracted_skills']:
            if s: all_skills.extend(s.split(", "))
            
        return Counter(all_skills).most_common(20)

if __name__ == "__main__":
    extractor = SkillExtractor()
    extractor.analyze_skills()
    print("\nТоп навыков в БД:")
    for skill, count in extractor.get_top_skills():
        print(f" - {skill}: {count}")
