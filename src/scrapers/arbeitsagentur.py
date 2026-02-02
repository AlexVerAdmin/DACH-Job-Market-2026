import requests
import time

class ArbeitsagenturScraper:
    def __init__(self):
        self.api_url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs"
        self.headers = {
            "X-API-Key": "jobboerse-jobsuche",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def fetch_jobs(self, query, pages=1, country="DE"):
        all_jobs = []
        for page in range(1, pages + 1):
            params = {"was": query, "wo": "Deutschland", "page": page, "size": 50}
            try:
                response = requests.get(self.api_url, params=params, headers=self.headers, timeout=15)
                if response.status_code != 200: break
                
                items = response.json().get('stellenangebote', [])
                for item in items:
                    ref_nr = item.get('refnr')
                    all_jobs.append({
                        'id': ref_nr,
                        'title': item.get('titel'),
                        'company': item.get('arbeitgeber', 'Unknown'),
                        'location': item.get('arbeitsort', {}).get('ort', 'Deutschland'),
                        'url': f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref_nr}",
                        'created': item.get('aktuelleVeroeffentlichungsdatum'),
                        'source': 'arbeitsagentur',
                        'country_search': country.upper()
                    })
                if len(items) < 50: break
                time.sleep(1)
            except Exception as e:
                print(f"  [!] Arbeitsagentur error: {e}")
                break
        return all_jobs
