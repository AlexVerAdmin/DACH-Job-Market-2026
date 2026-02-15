import requests
import os
from dotenv import load_dotenv

load_dotenv()

def check_adzuna_quota():
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    
    # Делаем один минимальный запрос для получения заголовков
    url = f"https://api.adzuna.com/v1/api/jobs/de/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 1,
        "what": "test"
    }
    
    try:
        response = requests.get(url, params=params)
        with open("adzuna_headers.txt", "w") as f:
            for key, value in response.headers.items():
                f.write(f"{key}: {value}\n")
        
        print("\n[OK] Все заголовки сохранены в adzuna_headers.txt")
        
        remaining = response.headers.get('X-RateLimit-Remaining')
        limit = response.headers.get('X-RateLimit-Limit')
        
        print("\n=== ADZUNA API QUOTA ===")
        if remaining:
            print(f"Осталось запросов: {remaining}")
        if limit:
            print(f"Общий лимит: {limit}")
        if not remaining and not limit:
            print("Заголовки лимитов не найдены. Ответ API:")
            print(response.headers)
        print("========================\n")
    except Exception as e:
        print(f"Ошибка при проверке квоты: {e}")

if __name__ == "__main__":
    check_adzuna_quota()
