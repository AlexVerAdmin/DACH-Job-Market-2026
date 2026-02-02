import requests
try:
    res = requests.get("http://localhost:8501", timeout=5)
    print(f"Status: {res.status_code}")
    print(f"Content length: {len(res.text)}")
    if "Streamlit" in res.text:
        print("Success: Streamlit is serving the page.")
    else:
        print("Warning: Streamlit keywords not found in response.")
except Exception as e:
    print(f"Error connecting to Streamlit: {e}")
