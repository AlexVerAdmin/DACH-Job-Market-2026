import requests
from bs4 import BeautifulSoup
import time
import re

class XingScraper:
    def __init__(self):
        self.base_url = "https://www.xing.com/jobs/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def _parse_salary(self, salary_str):
        if not salary_str: return None, None
        matches = re.findall(r'(\d{1,3}[\.\s]?\d{3})', salary_str)
        if matches:
            values = [float(m.replace('.', '').replace(' ', '')) for m in matches]
            values = [v for v in values if 20000 <= v <= 300000]
            if len(values) >= 2: return min(values), max(values)
            elif len(values) == 1: return values[0], values[0]
        return None, None

    def fetch_jobs(self, query, pages=1, country="DE"):
        all_jobs = []
        loc_map = {"DE": "Germany", "AT": "Austria", "CH": "Switzerland"}
        location = loc_map.get(country.upper(), "Germany")
        
        for page in range(pages):
            params = {
                "keywords": query, 
                "location": location,
                "offset": page * 20
            }
            try:
                response = requests.get(self.base_url, params=params, headers=self.headers, timeout=15)
                if response.status_code != 200: break
                
                soup = BeautifulSoup(response.text, 'html.parser')
                job_links = soup.find_all('a', href=re.compile(r'/jobs/.*-\d+$'))
                
                for link in job_links:
                    container = link.find_parent('article') or link
                    full_text = container.get_text("|", strip=True)
                    if "Jobs gefunden" in full_text[:20]: continue
                    
                    title_elem = container.find(['h2', 'h3'])
                    title = title_elem.get_text(strip=True) if title_elem else link.get_text(strip=True).split("|")[0]
                    
                    # Пытаемся найти компанию в специальных элементах или тексте
                    company = "Unknown"
                    company_elem = container.find('p', class_=re.compile(r'CompanyLine')) or \
                                   container.find('span', class_=re.compile(r'CompanyName'))
                    
                    if company_elem:
                        company = company_elem.get_text(strip=True)
                    else:
                        # Fallback: парсим из full_text, пропуская заголовок
                        parts = [p.strip() for p in full_text.split("|") if p.strip()]
                        # Обычно: [Заголовок, Компания, Локация, ...]
                        if len(parts) > 1:
                            if parts[0] == title and len(parts) > 2:
                                company = parts[1]
                                location = parts[2]
                            else:
                                company = parts[1]
                    
                    # Локация
                    location = "Germany"
                    loc_elem = container.find('p', class_=re.compile(r'LocationLine')) or \
                               container.find('span', class_=re.compile(r'LocationName'))
                    if loc_elem:
                        location = loc_elem.get_text(strip=True)
                    elif len(parts) > 2 and company != parts[2]:
                        location = parts[2]

                    # Очистка локации от лишнего текста (типа "• Hybrid")
                    location = location.split("•")[0].strip()
                    
                    # Пытаемся найти зарплату в тексте
                    salary_text = re.search(r'(\d[\d\.\s]*€|\d[\d\.\s]*\s*Euro|от\s*[\d\.\s]+)', full_text, re.I)
                    salary_min, salary_max = self._parse_salary(salary_text.group(0) if salary_text else "")
                    
                    url = link['href']
                    if url.startswith('/'): url = "https://www.xing.com" + url
                    
                    all_jobs.append({
                        'id': url.split('-')[-1],
                        'title': title,
                        'company': company,
                        'location': location,
                        'url': url,
                        'salary_min': salary_min,
                        'salary_max': salary_max,
                        'source': 'xing',
                        'country_search': country.upper()
                    })
                time.sleep(2)
            except Exception as e:
                print(f"  [!] Xing error: {e}")
                break
        return all_jobs
