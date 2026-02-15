import requests
from bs4 import BeautifulSoup
import time
import re

class StepStoneScraper:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def fetch_jobs(self, query, pages=1, country="DE"):
        all_jobs = []
        # Сопоставляем домены и локации
        domain_map = {
            "DE": {"url": "https://www.stepstone.de", "loc": "in-deutschland"},
            "AT": {"url": "https://www.stepstone.at", "loc": "in-österreich"},
            "CH": {"url": "https://www.stepstone.ch", "loc": "in-schweiz"}
        }
        
        cfg = domain_map.get(country.upper(), domain_map["DE"])
        
        for page in range(1, pages + 1):
            search_query = query.replace(' ', '-')
            url = f"{cfg['url']}/jobs/{search_query}/{cfg['loc']}?page={page}"
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                if response.status_code != 200: break
                
                soup = BeautifulSoup(response.text, "html.parser")
                for item in soup.find_all("article"):
                    title_elem = item.find("h2")
                    link_elem = item.find("a", href=re.compile(r"/stellenangebote--"))
                    if not title_elem or not link_elem: continue
                    
                    job_url = link_elem["href"]
                    if not job_url.startswith("http"): job_url = cfg['url'] + job_url
                    
                    # Извлечение ID
                    id_match = re.search(r"-(\d+)\.html", job_url)
                    job_id = id_match.group(1) if id_match else str(hash(job_url))
                    
                    company_elem = item.find("a", href=re.compile(r"/cmp/")) or \
                                   item.find("div", {"data-test": "job-item-company-name"})
                    company = company_elem.get_text(strip=True) if company_elem else "Unknown"
                    
                    location_elem = item.find("span", {"data-test": "job-item-location"}) or \
                                    item.find("div", {"data-test": "job-item-location"})
                    location = location_elem.get_text(strip=True) if location_elem else ("Germany" if country=="DE" else country)
                    
                    # Если компания Unknown, попробуем вытащить из URL
                    if company == "Unknown" and "--" in job_url:
                        # URL format: ...--Title-Location-Company--ID-inline.html
                        url_parts = job_url.split("--")[1].split("-")
                        if len(url_parts) > 2:
                            # Usually the last few parts before the ID
                            company = " ".join(url_parts[-3:-1]).replace("-", " ").title()

                    all_jobs.append({
                        "id": job_id,
                        "title": title_elem.get_text(strip=True),
                        "company": company,
                        "location": location,
                        "url": job_url,
                        "source": "stepstone",
                        "country_search": country.upper()
                    })
                time.sleep(2)
            except Exception as e:
                print(f"  [!] StepStone error: {e}")
                break
        return all_jobs
