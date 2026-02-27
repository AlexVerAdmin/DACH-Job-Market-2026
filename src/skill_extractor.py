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
        self.blacklist = {
            'THE', 'AND', 'FOR', 'WITH', 'FROM', 'THIS', 'THAT', 'YOUR', 'WILL', 'TEAM', 'DATA', 'WORK', 
            'PLEASE', 'FOLLOW', 'SELECT', 'COUNTRY', 'HOME', 'OFFICE', 'JOB', 'TASKS', 'PROFILE', 
            'OFFER', 'BENEFITS', 'UNITED', 'KINGDOM', 'AUSTRALIA', 'ÖSTERREICH', 'BELGIË', 'BRASIL', 
            'CANADA', 'FRANCE', 'DEUTSCHLAND', 'INDIA', 'ITALIA', 'MÉXICO', 'NEDERLAND', 'NEW ZEALAND', 
            'POLSKA', 'SINGAPORE', 'SOUTH AFRICA', 'ESPAÑA', 'SCHWEIZ', 'STATES', 'LOGIN', 'REGISTER', 
            'WIR', 'SIE', 'UND', 'MIT', 'DAS', 'DIESE', 'EINE', 'ODER', 'SIND', 'IHRE', 'BITTE', 'WÄHLEN',
            # Generic Business terms to ignore in Discovery
            'ADVISORY', 'ANALYST', 'DIGITAL', 'FUND', 'PERFORMANCE', 'TRANSFORMATION', 'TRANSPARENCY',
            'SERVICES', 'SOLUTION', 'MANAGEMENT', 'STRATEGY', 'PROJECT', 'BUSINESS', 'SENIOR', 'JUNIOR',
            'MANAGER', 'EXPERT', 'PROFESSIONAL', 'SUPPORT', 'GLOBAL', 'CENTRIC', 'FLEXIBLE', 'JOIN',
            'WORKING', 'RECRUITING', 'DEVELOPMENT', 'EMPLOYEE', 'OPPORTUNITY', 'RESPONSIBILITY',
            'ANALYTIC', 'ANALYSIS', 'ANALYST', 'ANALYSTIN', 'ENGINEER', 'DEVELOPER', 'CONSULTANT',
            # Web tracking / Junky patterns
            'GTM', 'UA-', 'GA-', 'COOKIE', 'CONSENT', 'PRIVACY', 'SECURITY', 'LOGOUT', 'BROWSER',
            'CHROME', 'SAFARI', 'FIREFOX', 'MOBILE', 'DESKTOP', 'TABLET', 'WINDOW', 'DOCUMENT',
            # Company names / Benefits / False positives
            'IABG', 'KITA', 'WELLPASS', 'QUALITRAIN', 'JOBCYCLE', 'EGYM', 'MYFITNESS', 'WWW',
            'BENEFIT', 'PENSION', 'INSURANCE', 'VACATION', 'CANTEEN', 'PARKING', 'BONUS', 'SALARY',
            'INTERVIEW', 'CAREER', 'ABOUT', 'CONTACT', 'IMPRINT', 'WEBSITE', 'SOCIAL', 'MEDIA',
            'WHATSAPP', 'TELEGRAM', 'SKYPE', 'LINKEDIN', 'XING', 'FACEBOOK', 'INSTAGRAM',
            # German Job Posting Headers / Generic Common Words
            'AUFGABEN', 'DEINE', 'DICH', 'ERWARTET', 'PROFIL', 'THEMEN', 'TECHNOLOGIEN', 'TECHNOLOGY',
            'VORAUSSETZUNGEN', 'UNSERE', 'ANFORDERUNGEN', 'IHRE', 'UNSER', 'IHRE', 'WIR', 'SIE',
            'SIND', 'EINE', 'ODER', 'EINES', 'DAS', 'DEM', 'DER', 'DIE', 'DEN', 'VON', 'BEI', 'FÜR',
            'MIT', 'EINEN', 'EINEM', 'EINER', 'EINE', 'AUF', 'ZUM', 'ZUR', 'SOWIE', 'WIE', 'AB',
            # German HR boilerplate / Benefits
            'ACADEMY', 'WEITERBILDUNG', 'WEITERBILDUNGEN', 'ALTSERSVORSORGE', 'ANSTELLUNG', 'VOLLZEIT', 
            'TEILZEIT', 'JOBBIKE', 'BACHELOR', 'MASTER', 'DIPLOM', 'STUDIUM', 'UNIVERSITÄT', 'HOCHSCHULE',
            'KARRIERE', 'BEWERBUNG', 'JOBRAD', 'WORKLIFE', 'BALANCE', 'MOBILES', 'ARBEITEN', 'HOMEOFFICE',
            'FLEXIBEL', 'FLEXIBILITÄT', 'MITEINANDER', 'VERANTWORTUNG', 'ENGAGEMENT', 'LEIDENSCHAFT',
            'EXPERTISE', 'TEAMS', 'BERATUNG', 'KUNDEN', 'PROJEKTE', 'ERFOLG', 'ZUKUNFT', 'BDE',
            'VL', 'VERMÖGENSWIRKSAME', 'LEISTUNGEN', 'ALTRSVERSORGUNG', 'PENSION', 'RETIREMENT',
            'KITA', 'CARE', 'SUPPORT', 'AMAZON', 'GOOGLE', 'MICROSOFT'
        }

    def _is_garbage(self, s):
        """Checks if a string looks like web junk, random IDs, URLs, emails or generic contact info."""
        if not s: return True
        s_upper = s.upper()
        s_lower = s.lower()
        if s_upper in self.blacklist: return True

        # 0. Reject pure numbers, prices, or ranges (e.g. 60.000, 75.000, 10-20)
        if re.match(r'^[\d\s\.,€$£-]+$', s) and len(re.sub(r'[^\d]', '', s)) >= 3:
            return True
        
        # 1. Ignore URLs, domains and emails (e.g. freenet.ag, nils.meissner@...)
        if re.search(r'\.(de|com|at|org|net|ag|me|io|info|pro|group|eu|uk|ch)$', s_lower):
            return True
        if '@' in s_lower or s_lower.startswith('www.') or 'http' in s_lower:
            return True
        
        # 2. Ignore pure digits or common HR slash patterns
        if '/' in s and len(s) < 30: 
            parts = s.split('/')
            for p in parts:
                p_clean = p.strip().upper()
                if p_clean in self.blacklist or len(p_clean) < 2 or p_clean.isdigit(): return True
            hr_keywords = {'ANSTELLUNG', 'VOLLZEIT', 'TEILZEIT', 'BACHELOR', 'MASTER', 'DIRECT', 'BEREICH', 'LEVEL'}
            if any(p.upper() in hr_keywords for p in parts): return True
        
        # 3. Handle names or identifiers in format "first.last" (e.g. nils.meissner, Co.KG)
        if '.' in s and len(s) < 30:
            parts = s.split('.')
            if len(parts) == 2 and all(p.islower() and len(p) > 1 for p in parts):
                return True
            if s_upper in ['CO.KG', 'GMBH', 'GMBH.CO', 'KARRIERE', 'BEWERBUNG']: return True
            if any(p.upper() in self.blacklist for p in parts): return True
        
        # 4. Handle 3-letter strings - tighten the "Discovery"
        if len(s) == 3 and s.isupper():
            believable_3 = {
                'API', 'CLI', 'GUI', 'NPM', 'SSH', 'SSL', 'TCP', 'UDP', 'XML', 'DOM', 'SDK',
                'NLP', 'LLM', 'OCR', 'RPA', 'ERP', 'CRM', 'BI', 'CI', 'CD', 'UI', 'UX',
                'AWS', 'SQL', 'SAP', 'GCP', 'ETL', 'GIT', 'VPC', 'IAM', 'CDN'
            }
            if s_upper not in believable_3:
                vowels = 'AEIOU'
                if not any(v in s_upper for v in vowels): return True
                return True # Default to skip unknown 3-letter acronyms
        
        # 5. Filter CamelCase that looks like benefits or common German words
        if any(c.isdigit() for c in s) and any(c.islower() for c in s) and any(c.isupper() for c in s):
            if len(s) < 10 and s_upper.startswith('GPT'): return False
            return True
            
        # 6. Check prefix/suffix or generic noise
        if s.startswith(('.', '/')) or s.endswith(('.', '/')): return True
        if s_lower in ['whatsapp', 'telegram', 'viber', 'skype', 'linkedin', 'xing', 'facebook']: return True

        # 7. Check if it's a known non-technical word
        de_noise = {'DYNAMISCH', 'MOTIVIERT', 'ERFAHREN', 'EIGENSTÄNDIG', 'KREATIV', 'OFFEN', 'BUNT'}
        if s_upper in de_noise: return True

        return False

    def extract_from_text(self, text):
        """Analyzes a single block of text (title + description) for skills."""
        if not text: return []
        
        # Pre-process text to handle common German "gender-neutral" suffixes
        text = re.sub(r'(/in|\*in|:in)\b', '', text, flags=re.IGNORECASE)
        
        lower_text = text.lower()
        found_skills = set()
        
        # 1. Поиск установленных навыков (Case-insensitive)
        for skill, pattern in self.skills_patterns.items():
            if re.search(pattern, lower_text):
                found_skills.add(skill)
        
        # 2. Discovery: Поиск неизвестных технических терминов
        # Focus on CamelCase, acronyms 3+, or terms with dots/slashes
        # Updated regex to require letters in slash/dot terms to avoid numbers
        candidates = re.findall(r'\b[A-Z][a-z]+[A-Z][A-Za-z0-9]*\b|\b[A-Z]{3,12}\b|\b[A-Za-z]{2,}[./][A-Za-z]{2,}\b', text)
        
        def normalize_for_check(s):
            return re.sub(r'[^a-z0-9]', '', s.lower())

        for cond in candidates:
            cond_clean = cond.strip('.,/() -')
            if len(cond_clean) < 2: continue
            if cond_clean.upper() in self.blacklist: continue
            if self._is_garbage(cond_clean): continue
            
            # Normalization check to avoid "Power BI" vs "PowerBI"
            cond_norm = normalize_for_check(cond_clean)
            is_known = False
            for known in found_skills:
                known_norm = normalize_for_check(known)
                if cond_norm == known_norm or cond_norm in known_norm:
                    is_known = True
                    break
            
            if not is_known:
                found_skills.add(cond_clean)
        
        return sorted(list(found_skills))


    def analyze_skills(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT signature, title, description, source FROM vacancies", conn)
        conn.close()

        print(f"[Skills] Анализ {len(df)} вакансий (Discovery mode ON)...")
        
        results = []
        for idx, row in df.iterrows():
            orig_text = f"{row['title']} {row['description']}"
            skills_list = self.extract_from_text(orig_text)
            results.append({
                'signature': row['signature'],
                'skills': ", ".join(skills_list)
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
