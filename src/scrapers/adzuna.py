import requests
import time
import os
from dotenv import load_dotenv
from datetime import datetime
from scrapers.api_usage_tracker import ApiUsageTracker

load_dotenv()

class AdzunaScraper:
    def __init__(self):
        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = os.getenv("ADZUNA_APP_KEY")
        self.base_url = "https://api.adzuna.com/v1/api/jobs"
        self.usage = ApiUsageTracker()
        self.LIMITS = {
            "minute": 25,
            "daily": 250,
            "weekly": 1000,
            "monthly": 2500
        }

    def fetch_jobs(self, country="de", query="Data Analyst", pages=1, results_per_page=50):
        all_jobs = []
        for page in range(1, pages + 1):
            status = self.usage.get_status()
            over_limit = False
            for period, count in status.items():
                if count >= self.LIMITS.get(period, 999999):
                    print(f"    [!] Adzuna limit reached: {period.capitalize()} hits = {count}/{self.LIMITS[period]}")
                    over_limit = True
                    break
            if over_limit: break

            url = f"{self.base_url}/{country.lower()}/search/{page}"
            params = {
                "app_id": self.app_id,
                "app_key": self.app_key,
                "results_per_page": results_per_page,
                "what": query,
                "content-type": "application/json"
            }
            try:
                self.usage.track_hit()
                response = requests.get(url, params=params, timeout=10)
                self.remaining_calls = response.headers.get('X-RateLimit-Remaining', 'N/A')
                
                if response.status_code == 404: break
                response.raise_for_status()
                
                results = response.json().get('results', [])
                for job in results:
                    # Нормализуем данные под общий формат
                    job['source'] = 'adzuna'
                    job['country_search'] = country.upper()
                
                all_jobs.extend(results)
                if len(results) < results_per_page: break
                time.sleep(1)
            except Exception as e:
                print(f"  [!] Adzuna API error ({country}): {e}")
                break
        return all_jobs
    def fetch_salary_history(self, country="de", query="Data Analyst"):
        """
        Получает исторические данные о зарплатах (тренды) для указанной роли и страны.
        Использует эндпоинт /history.
        """
        status = self.usage.get_status()
        for period, count in status.items():
            if count >= self.LIMITS.get(period, 999999):
                print(f"    [!] Adzuna limit reached: {period.capitalize()} hits = {count}/{self.LIMITS[period]}")
                return {}

        url = f"{self.base_url}/{country.lower()}/history"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key, 
            "what": query,
            "content-type": "application/json"
        }
        try:
            self.usage.track_hit()
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                month_data = data.get('month', {})
                if not month_data:
                    print(f"    [!] Adzuna History: No data found for '{query}' in {country.upper()}")
                return month_data
            else:
                print(f"    [!] Adzuna History Error: Status {response.status_code} for '{query}' in {country.upper()}")
                return {}
        except Exception as e:
            print(f"    [!] Adzuna History Exception ({query}, {country}): {e}")
            return {}
