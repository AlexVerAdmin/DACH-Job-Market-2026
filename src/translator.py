import os
import requests
import sqlite3
import re
import time
import configparser
from dotenv import load_dotenv

load_dotenv()

class JobTranslator:
    def __init__(self, db_path="data/jobs_database.sqlite"):
        self.db_path = db_path
        self.api_key = os.getenv("DEEPL_API_KEY")
        self.url = "https://api-free.deepl.com/v2/translate" # Use "https://api.deepl.com/v2/translate" for Pro
        self.usage_url = "https://api-free.deepl.com/v2/usage"
        self.chars_translated = 0
        
        # Priority: Absolutely positioned settings.ini -> .env -> default 5000
        config = configparser.ConfigParser()
        # Find settings.ini in the root (one level up from src/)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, 'settings.ini')
        config.read(config_path, encoding='utf-8')
        
        self.SESSION_LIMIT = config.getint('DeepL', 'session_limit', 
                                          fallback=int(os.getenv("DEEPL_SESSION_LIMIT", 5000)))
        print(f"  [Translator] Session limit loaded: {self.SESSION_LIMIT} characters.")

    def clean_title(self, title):
        """Removes gender suffixes and remote noise to improve grouping."""
        if not title: return ""
        # Remove common gender suffixes
        title = re.sub(r'\s*[\(/]?\s*(m/w/d|f/m/d|w/m/d|gn|m/f/d)\s*[\)]?\s*', ' ', title, flags=re.I)
        # Remove location/remote markers that don't change the job role
        title = re.sub(r'\s*[\|\-/]?\s*(100%\s*remote|Homeoffice|Home-Office|Hybrid|Remote|on-site)\s*', ' ', title, flags=re.I)
        # Remove extra spaces
        title = re.sub(r'\s+', ' ', title).strip()
        return title

    def get_api_usage(self):
        """Fetch current monthly usage from DeepL API."""
        if not self.api_key: return None
        headers = {"Authorization": f"DeepL-Auth-Key {self.api_key}"}
        try:
            response = requests.get(self.usage_url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return {
                    "used": data.get("character_count", 0),
                    "limit": data.get("character_limit", 500000)
                }
            else:
                print(f"  [!] DeepL Usage Error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            print(f"  [!] DeepL Usage Exception: {e}")
            return None

    def translate_titles(self, target_lang="RU", limit=1000):
        if not self.api_key:
            print("[!] DeepL API Key not found in .env. Skipping translation.")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check monthly usage before starting
        usage = self.get_api_usage()
        if usage:
            print(f"[Translator] Monthly DeepL Usage: {usage['used']}/{usage['limit']} chars")
            if usage['used'] >= usage['limit']:
                print("[!] Monthly limit reached! Stop.")
                conn.close()
                return

        # 1. Skip 'Senior' level vacancies to save credits (as per user request)
        # We set translated_title = title for them so they are marked as 'processed'
        cursor.execute("UPDATE vacancies SET translated_title = title WHERE search_level = 'Senior' AND translated_title IS NULL")
        conn.commit()

        # 2. Fetch ALL other untranslated titles to perform smart grouping
        cursor.execute("SELECT signature, title FROM vacancies WHERE translated_title IS NULL")
        rows = cursor.fetchall()
        if not rows:
            print("[Translator] No new titles to translate.")
            conn.close()
            return

        # Group signatures by CLEANED title to save credits
        groups = {}
        for sig, title in rows:
            clean = self.clean_title(title)
            if not clean: continue
            if clean not in groups:
                groups[clean] = []
            groups[clean].append(sig)

        print(f"[Translator] Found {len(rows)} vacancies, reduced to {len(groups)} unique cleaned titles.")
        print(f"[Translator] Starting session (Limit: {self.SESSION_LIMIT} chars per run)...")

        processed_count = 0
        consecutive_errors = 0
        for cleaned_title, signatures in groups.items():
            # Check session limit
            title_len = len(cleaned_title)
            if self.chars_translated + title_len > self.SESSION_LIMIT:
                print(f"[Translator] Session limit reached ({self.chars_translated}/{self.SESSION_LIMIT} chars). Stopping.")
                break

            translated = self._call_deepl(cleaned_title, target_lang)
            if translated:
                consecutive_errors = 0
                self.chars_translated += title_len
                # Update all vacancies that share this cleaned title
                placeholders = ', '.join(['?'] * len(signatures))
                cursor.execute(
                    f"UPDATE vacancies SET translated_title = ? WHERE signature IN ({placeholders})",
                    [translated] + signatures
                )
                processed_count += len(signatures)
                print(f"  [+] ({self.chars_translated}/{self.SESSION_LIMIT}) {cleaned_title} -> {translated} ({len(signatures)} vacancies)")
            else:
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    print("[Translator] Too many consecutive errors. Stopping session for safety.")
                    break
            
            # Add a tiny delay to prevent HTTP 429 (Rate Limit)
            time.sleep(0.3)

        conn.commit()
        conn.close()
        
        final_usage = self.get_api_usage()
        usage_str = f" (Monthly total: {final_usage['used']}/{final_usage['limit']})" if final_usage else ""
        print(f"[Translator] Finished. Updated {processed_count} vacancies using {self.chars_translated} characters.{usage_str}")

    def _call_deepl(self, text, target_lang):
        headers = {"Authorization": f"DeepL-Auth-Key {self.api_key}"}
        data = {
            "text": text,
            "target_lang": target_lang
        }
        try:
            response = requests.post(self.url, headers=headers, data=data, timeout=10)
            if response.status_code == 200:
                result = response.json()
                return result["translations"][0]["text"]
            else:
                print(f"  [!] DeepL Error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            print(f"  [!] Translation Exception: {e}")
            return None
