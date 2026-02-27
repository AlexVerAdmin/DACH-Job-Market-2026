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
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
        ]
        self.referers = [
            "https://www.google.com/",
            "https://www.google.de/",
            "https://www.bing.com/",
            "https://www.adzuna.de/",
            "https://www.stepstone.de/"
        ]

    def get_headers(self, url=None):
        ua = random.choice(self.user_agents)
        ref = random.choice(self.referers)
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": ref,
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1"
        }
        return headers

    def get_pending_vacancies(self, limit=100, source=None):
        # Ищем вакансии, где описание слишком короткое (< 600 символов) 
        # или совпадает с заголовком (что часто бывает у агрегаторов)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT signature, url, source, title FROM vacancies 
            WHERE (length(description) < 600 OR description = title)
            AND is_active = 1
        '''
        params = []
        if source:
            query += " AND source = ?"
            params.append(source)
            
        query += " ORDER BY last_seen DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, tuple(params))
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

    def scrape_adzuna_redirect(self, url, session=None):
        try:
            # Используем общую сессию, если передана, иначе создаем новую
            if session is None:
                session = requests.Session()
                
            res = session.get(url, headers=self.get_headers(url), timeout=15, allow_redirects=True)
            
            # Если словили 403, пробуем еще разок через паузу с другим UA
            if res.status_code == 403:
                time.sleep(random.uniform(2, 5))
                res = session.get(url, headers=self.get_headers(url), timeout=15, allow_redirects=True)

            if res.status_code == 403:
                return res.url, "ERR_403"
            if res.status_code == 404:
                return res.url, "ERR_404"
            
            # Проверка на капчу в тексте
            text_low = res.text.lower()
            if any(marker in text_low for marker in ["captcha", "blocked", "suspicious behavior", "verdächtiges verhalten"]):
                return res.url, "ERR_403"
                
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

        if not desc or len(desc) < 300:
            return False
            
        # Защита от мусора (страниц блокировки, капчи, списков стран)
        garbage_markers = [
            "verdächtiges verhalten", "suspicious behavior", "captcha",
            "bitte das richtige land auswählen", "choose the correct country",
            "united kingdom australia österreich", "systeme haben ungewöhnliches verhalten",
            "access denied", "access to this page has been denied"
        ]
        if any(marker in desc.lower() for marker in garbage_markers):
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

    def _process_one(self, row, session):
        sig, url, source, title = row
        try:
            # Рандомная пауза перед запросом, чтобы не "долбить" сервер
            time.sleep(random.uniform(1.0, 4.0))
            final_url, html = self.scrape_adzuna_redirect(url, session=session)
            
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
            
            return "ok_no_change" 
        except Exception as e:
            return f"error_{type(e).__name__}"

    def run_parallel(self, limit=50, max_workers=5, source=None):
        pending = self.get_pending_vacancies(limit, source=source)
        total_pending = len(pending)
        if total_pending == 0:
            print(f"[Desc] Нет вакансий для обогащения{f' ({source})' if source else ''}.")
            return 0
            
        print(f"[Desc] Обработка {total_pending} вакансий в {max_workers} потоках{f' ({source})' if source else ''}...")
        
        stats = {
            "ok": 0, "ok_no_change": 0, "403_forbidden": 0, "404_not_found": 0, 
            "parsing_failed": 0, "too_short": 0, "connection_error": 0
        }
        
        last_reported = 0
        consecutive_403 = 0
        
        # Используем одну сессию на все запросы (более экономно)
        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {executor.submit(self._process_one, row, session): row for row in pending}
                for future in as_completed(future_to_url):
                    res = future.result()
                    
                    if res == "403_forbidden":
                        consecutive_403 += 1
                    else:
                        if res != "ok_no_change": # Пропускаем "уже актуально" для счетчика ошибок
                            consecutive_403 = 0
                            
                    if res in stats:
                        stats[res] += 1
                    elif str(res).startswith("error"):
                        stats["error"] = stats.get("error", 0) + 1
                    else:
                        stats["other"] = stats.get("other", 0) + 1
                    
                    # Если получаем слишком много 403 подряд, останавливаемся
                    if consecutive_403 > 15:
                        print("\n[!] Обнаружена массовая блокировка (403). Останавливаем процесс...")
                        break

                    # Печатаем прогресс каждые 20 штук
                    processed = sum(stats.values())
                    current_milestone = (processed // 20) * 20
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

    def extract_salary_from_text(self, text):
        """
        Regex-based salary extraction for German/English postings.
        Returns (min_salary, max_salary) or (None, None).
        """
        if not text: return None, None
        
        # 1. Look for patterns like "50.000 - 80.000" or "45.000 EUR"
        # Support dots/spaces as thousands separators
        salary_pattern = re.compile(r'(\d{1,3}[\.\s]?\d{3})\s?[-–—то]\s?(\d{1,3}[\.\s]?\d{3})')
        matches = salary_pattern.findall(text)
        
        candidates = []
        for m in matches:
            try:
                val1 = float(m[0].replace('.', '').replace(' ', ''))
                val2 = float(m[1].replace('.', '').replace(' ', ''))
                if 25000 <= val1 <= 250000 and 25000 <= val2 <= 250000:
                    candidates.append((val1, val2))
            except: continue
            
        if candidates:
            # Return the highest range found
            return max(candidates, key=lambda x: x[0])
            
        # 2. Single value matching (e.g. "ab 50.000€" or "bis zu 70k")
        single_pattern = re.compile(r'(?:ab|bis|zu|bis\szu|around|salary|gehalt|около|от|до)\s?(\d{1,3}[\.\s]?\d{3}|[4-9]\d\s?k)', re.I)
        matches = single_pattern.findall(text)
        for m in matches:
            try:
                val = m.lower().replace('k', '000').replace('.', '').replace(' ', '')
                val = float(val)
                if 30000 <= val <= 250000:
                    return val, val
            except: continue
            
        return None, None


if __name__ == "__main__":

    manager = DescriptionManager()
    manager.run_parallel(limit=20, max_workers=5)
