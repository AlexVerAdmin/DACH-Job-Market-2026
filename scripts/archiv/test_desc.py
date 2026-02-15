from description_manager import DescriptionManager
import requests

def test_single_url(url):
    print(f"Testing URL: {url}")
    dm = DescriptionManager()
    
    # 1. Пробуем редирект
    final_url, html = dm.scrape_adzuna_redirect(url)
    print(f"Final URL: {final_url}")
    if html:
        print(f"HTML Length: {len(html)}")
        # 2. Пробуем извлечь описание
        row = (None, url, 'adzuna', 'Test Title')
        success = dm._process_one(row)
        print(f"Success: {success}")
    else:
        print("Failed to get HTML")

if __name__ == "__main__":
    url = "https://www.adzuna.de/details/5015651555?utm_medium=api&utm_source=5984b7e2"
    test_single_url(url)
