import requests

url = "https://www.stepstone.de/stellenangebote--Junior-Data-Analyst-w-m-d-Koeln-MSR-Consulting-Group-GmbH--13514008-inline.html"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

try:
    print(f"Fetching {url}...")
    res = requests.get(url, headers=headers, timeout=10)
    print(f"Status: {res.status_code}")
    print(f"Final URL: {res.url}")
    print(f"Content length: {len(res.text)}")
    print(f"First 500 chars of HTML: {res.text[:500]}")
    
    with open("test_response.html", "w", encoding="utf-8") as f:
        f.write(res.text)
        
    keyword = "JobDescription"
    if keyword in res.text:
        print(f"Keyword '{keyword}' found in HTML")
    else:
        print(f"Keyword '{keyword}' NOT found in HTML")
        
    keyword2 = "js-app-ld-ContentBlock"
    if keyword2 in res.text:
        print(f"Keyword '{keyword2}' found in HTML")
    else:
        print(f"Keyword '{keyword2}' NOT found in HTML")

except Exception as e:
    print(f"Error: {e}")
