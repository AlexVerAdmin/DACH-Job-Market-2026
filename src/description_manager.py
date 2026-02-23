import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

class DescriptionManager:
    def __init__(self, db_path="data/jobs_database.sqlite"):
        self.db_path = db_path
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def get_pending_vacancies(self, limit=100):
        # Ищем вакансии, где описание слишком короткое (< 600 символов) 
        # или совпадает с заголовком (что часто бывает у агрегаторов)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT signature, url, source, title FROM vacancies 
            WHERE (length(description) < 600 OR description = title)
            AND is_active = 1
            ORDER BY last_seen DESC
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return rows

    def scrape_json_ld(self, html):
        """
        Универсальный помощник для извлечения данных из JSON-LD.
        Возвращает словарь с найденными полями.
        """
        results = {"description": None, "salary_min": None, "salary_max": None}
        try:
            soup = BeautifulSoup(html, 'html.parser')
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    import json
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if isinstance(item, dict) and item.get('@type') == 'JobPosting':
                            # 1. Описание
                            if 'description' in item:
                                content_html = item['description']
                                clean_soup = BeautifulSoup(content_html, 'html.parser')
                                results["description"] = clean_soup.get_text(" ", strip=True)
                            
                            # 2. Зарплата (baseSalary)
                            salary = item.get('baseSalary')
                            if isinstance(salary, dict):
                                value = salary.get('value')
                                if isinstance(value, dict):
                                    results["salary_min"] = value.get('minValue') or value.get('value')
                                    results["salary_max"] = value.get('maxValue') or value.get('value')
                            
                            # Если нашли главные данные, можно выходить (обычно JobPosting один на странице)
                            if results["description"]:
                                return results
                except:
                    continue
        except:
            pass
        return results if results["description"] else None

    def scrape_stepstone(self, html):
        """
        Извлекает данные вакансии с сайта StepStone.
        """
        # 1. Сначала пробуем JSON-LD
        data = self.scrape_json_ld(html)
        if data: return data

        # 2. Fallback: Старые CSS селекторы (только для описания)
        try:
            soup = BeautifulSoup(html, 'html.parser')
            content = soup.find('div', class_=lambda x: x and 'JobDescription' in x)
            if not content:
                content = soup.find('div', class_='js-app-ld-ContentBlock')
            
            if content:
                return {"description": content.get_text(" ", strip=True), "salary_min": None, "salary_max": None}
        except:
            pass
        return None

    def scrape_adzuna_redirect(self, url):
        try:
            res = requests.get(url, headers=self.headers, timeout=10, allow_redirects=True)
            if res.status_code == 403:
                return res.url, "ERR_403"
            if res.status_code == 404:
                return res.url, "ERR_404"
            return res.url, res.text
        except:
            return url, None

    def update_vacancy_fields(self, signature, data):
        """
        Обновляет описание и зарплату в базе, только если новые данные "лучше".
        """
        desc = data.get('description')
        s_min = data.get('salary_min')
        s_max = data.get('salary_max')

        if not desc or len(desc) < 100:
            return False
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Обновляем описание, если оно длиннее текущего
        cursor.execute('''
            UPDATE vacancies SET 
                description = CASE 
                    WHEN length(?) > length(description) THEN ? 
                    ELSE description 
                END,
                salary_min = COALESCE(salary_min, ?),
                salary_max = COALESCE(salary_max, ?)
            WHERE signature = ?
        ''', (desc, desc, s_min, s_max, signature))
        
        updated = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return updated

    def scrape_xing(self, html):
        # 1. Пробуем JSON-LD
        data = self.scrape_json_ld(html)
        if data: return data

        # 2. Fallback: Селекторы
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.find('div', class_=re.compile(r'job-description|description'))
        if not content:
            content = soup.find('main')
        
        if content:
            return {"description": content.get_text(" ", strip=True), "salary_min": None, "salary_max": None}
        return None

    def scrape_arbeitsagentur(self, html):
        """
        Извлекает описание с сайта Arbeitsagentur (обычно через JSON-LD).
        """
        return self.scrape_json_ld(html)

    def _process_one(self, row):
        sig, url, source, title = row
        try:
            final_url, html = self.scrape_adzuna_redirect(url)
            
            if html == "ERR_403": return "403_forbidden"
            if html == "ERR_404": return "404_not_found"
            if not html: return "connection_error"
            
            data = None
            if 'stepstone.de' in final_url:
                data = self.scrape_stepstone(html)
            elif 'xing.com' in final_url:
                data = self.scrape_xing(html)
            elif 'arbeitsagentur.de' in final_url:
                data = self.scrape_arbeitsagentur(html)
            else:
                data = self.scrape_json_ld(html)
                if not data:
                    soup = BeautifulSoup(html, 'html.parser')
                    for tag in ['script', 'style', 'nav', 'footer', 'header', 'aside']:
                        for match in soup.find_all(tag): match.decompose()
                    paragraphs = soup.find_all(['p', 'div', 'li'])
                    text_blocks = [p.get_text(" ", strip=True) for p in paragraphs if len(p.get_text()) > 100]
                    desc = "\n".join(text_blocks)
                    if desc:
                        data = {"description": desc, "salary_min": None, "salary_max": None}
            
            if not data or not data.get('description'):
                return "parsing_failed"
                
            if len(data['description']) < 500:
                return "too_short"

            if self.update_vacancy_fields(sig, data):
                return "ok"
            
            return "ok_no_change" # Описание уже было длинным
        except Exception as e:
            return f"error_{type(e).__name__}"

    def run_parallel(self, limit=50, max_workers=5):
        pending = self.get_pending_vacancies(limit)
        total_pending = len(pending)
        print(f"[Desc] Обработка {total_pending} вакансий в {max_workers} потоках...")
        
        stats = {
            "ok": 0, "ok_no_change": 0, "403_forbidden": 0, "404_not_found": 0, 
            "parsing_failed": 0, "too_short": 0, "connection_error": 0
        }
        
        last_reported = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(self._process_one, row): row for row in pending}
            for future in as_completed(future_to_url):
                res = future.result()
                if res in stats:
                    stats[res] += 1
                elif str(res).startswith("error"):
                    stats["error"] = stats.get("error", 0) + 1
                else:
                    stats["other"] = stats.get("other", 0) + 1
                
                # Печатаем прогресс каждые 50 штук
                processed = sum(stats.values())
                current_milestone = (processed // 50) * 50
                if current_milestone > last_reported:
                    print(f"  [Progress] Обработано: {processed}/{total_pending}...")
                    last_reported = current_milestone
        
        print("\n[Desc] Завершено. Результаты:")
        print(f"  [+] Успешно обновлено: {stats['ok']}")
        if stats['ok_no_change'] > 0: print(f"  [~] Уже актуально: {stats['ok_no_change']}")
        if stats['403_forbidden'] > 0: print(f"  [!] Заблокировано (403): {stats['403_forbidden']}")
        if stats['404_not_found'] > 0: print(f"  [!] Не найдено (404): {stats['404_not_found']}")
        if stats['too_short'] > 0: print(f"  [-] Слишком короткие: {stats['too_short']}")
        if stats['parsing_failed'] > 0: print(f"  [-] Не удалось извлечь: {stats['parsing_failed']}")
        if stats['connection_error'] > 0: print(f"  [?] Ошибка сети: {stats['connection_error']}")
        if stats.get('error', 0) > 0: print(f"  [!] Ошибок скрипта: {stats['error']}")
        
        return stats['ok']

if __name__ == "__main__":
    manager = DescriptionManager()
    manager.run_parallel(limit=20, max_workers=5)
