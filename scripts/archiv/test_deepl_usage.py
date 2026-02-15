import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from translator import JobTranslator
from dotenv import load_dotenv

load_dotenv()

def check_usage():
    print("=== DeepL Usage Check ===")
    key = os.getenv("DEEPL_API_KEY")
    if not key:
        print("[!] ERROR: DEEPL_API_KEY not found in .env file.")
        return

    translator = JobTranslator()
    usage = translator.get_api_usage()
    
    if usage:
        print(f"[+] Connection successful!")
        print(f"[+] Characters used this month: {usage['used']}")
        print(f"[+] Monthly limit: {usage['limit']}")
        remaining = usage['limit'] - usage['used']
        print(f"[+] Remaining: {remaining}")
    else:
        print("[!] Failed to fetch usage. Check your API key or internet connection.")
        print("[!] Note: This script uses the FREE API endpoint (api-free.deepl.com).")

if __name__ == "__main__":
    check_usage()
