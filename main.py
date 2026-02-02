# Ensure imports work when running from the project root
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from database_manager import DatabaseManager
from scrapers.adzuna import AdzunaScraper
from scrapers.stepstone import StepStoneScraper
from scrapers.xing import XingScraper
from scrapers.arbeitsagentur import ArbeitsagenturScraper
from description_manager import DescriptionManager
from data_utils import normalize_location

# === CONFIGURATION ===
CONFIG = {
    "ROLES": [
        "Data Analyst", "Data Scientist", "BI Analyst", 
        "AI Manager", "AI Project Manager", "Prompt Engineer",
        "Machine Learning Engineer", "Data Engineer"
    ],
    "LEVELS": {
        "Junior": ["Junior", "Entry Level", "Absolvent", "Trainee", "Graduate"],
        "Senior": ["Senior", "Lead", "Principal", "Expert"],
        "Intern": ["Internship", "Intern", "Working Student", "Thesis"],
        "General": [""]
    },
    "COUNTRIES": ["de", "at", "ch"],
    "DEFAULT_PAGES": {
        "priority": 5,      # StepStone, Xing
        "aggregator": 3     # Adzuna
    }
}

class Pipeline:
    def __init__(self, is_test=False):
        self.db = DatabaseManager()
        self.scrapers = {
            "adzuna": AdzunaScraper(),
            "stepstone": StepStoneScraper(),
            "xing": XingScraper(),
            "aa": ArbeitsagenturScraper()
        }
        self.is_test = is_test
        if is_test:
            CONFIG["ROLES"] = ["Data Analyst"]
            CONFIG["LEVELS"] = {"Junior": ["Junior"], "General": [""]}
            CONFIG["DEFAULT_PAGES"] = {"priority": 1, "aggregator": 1}

    def run(self, scrape=True, enrich=True, skills=True):
        print(f"=== STARTING PIPELINE: {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
        start_time = time.time()
        
        if scrape:
            print("\n[SCRAPING] Fetching new vacancies...")
            for role in CONFIG["ROLES"]:
                # --- 1. Direct Sources (High Accuracy) ---
                for level_name, synonyms in CONFIG["LEVELS"].items():
                    for syn in synonyms:
                        query = f"{syn} {role}".strip()
                        print(f"\n[QUERY] Searching for: '{query}'")
                        
                        for country in CONFIG["COUNTRIES"]:
                            # StepStone
                            print(f"  [STEPSTONE] Fetching ({country.upper()})...")
                            self._process_scraper("stepstone", query, role, level_name, country)
                            
                            # Xing
                            print(f"  [XING] Fetching...")
                            self._process_scraper("xing", query, role, level_name, country)

                            # Arbeitsagentur (DE only)
                            if country == "de":
                                print(f"  [AA] Fetching...")
                                self._process_scraper("aa", query, role, level_name, country)

                # --- 2. Aggregator (Efficiency: 1 broad query per role) ---
                for country in CONFIG["COUNTRIES"]:
                    print(f"\n  [ADZUNA] Fetching broad results for '{role}' ({country.upper()})...")
                    self._process_scraper("adzuna", role, role, "General", country, is_aggregator=True, auto_level=True)

        # 3. Description Enrichment (Critical for skill analysis)
        if enrich:
            print("\n[ENRICHMENT] Scraping full descriptions...")
            desc_manager = DescriptionManager(db_path=self.db.db_path)
            # Fetch pending descriptions
            limit = 20 if self.is_test else 1000
            desc_manager.run_parallel(limit=limit, max_workers=10)

        # 4. Skill Extraction based on full descriptions
        if skills:
            try:
                from skill_extractor import SkillExtractor
                print("\n[SKILLS] Extracting skills...")
                extractor = SkillExtractor(db_path=self.db.db_path)
                extractor.analyze_skills()
            except ImportError:
                print("\n[!] SkillExtractor not found. Skipping skill extraction.")

        end_time = time.time()
        print(f"\n=== PIPELINE FINISHED IN {round((end_time - start_time)/60, 1)} MINUTES ===")

    def _process_scraper(self, name, query, role, level_name, country, is_aggregator=False, auto_level=False):
        try:
            pages = CONFIG["DEFAULT_PAGES"]["aggregator" if is_aggregator else "priority"]
            
            if name in ["stepstone", "xing"]:
                jobs = self.scrapers[name].fetch_jobs(query, pages=pages, country=country)
            elif name == "adzuna":
                jobs = self.scrapers[name].fetch_jobs(country=country, query=query, pages=pages)
            else:
                jobs = self.scrapers[name].fetch_jobs(query, pages=pages, country=country)
            
            for j in jobs:
                j['search_query'] = role
                
                if auto_level:
                    # Detect level based on title
                    detected_level = "General"
                    title_text = j.get('title', '').lower()
                    for level, syns in CONFIG["LEVELS"].items():
                        if any(s.lower() in title_text for s in syns if s):
                            detected_level = level
                            break
                    j['search_level'] = detected_level
                else:
                    j['search_level'] = level_name
                    
                if 'country_search' not in j:
                    j['country_search'] = country.upper()
            
            added = self.db.save_vacancies(jobs)
            if added > 0:
                print(f"    [+] Added {added} new vacancies")
        except Exception as e:
            print(f"    [!] Error in {name}: {e}")

    def run_salary_trends(self):
        """
        Collect historical salary trends for all roles and countries.
        """
        print(f"\n=== STARTING SALARY TRENDS COLLECTION: {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
        # Adzuna is the main source for history
        adzuna = self.scrapers.get("adzuna")
        if not adzuna:
            print("  [!] Adzuna scraper not initialized.")
            return

        # Mapping roles to broad keywords that work better in Adzuna History API
        TREND_KEYWORDS_MAP = {
            "Data Analyst": ["Analyst", "Data"],
            "Data Scientist": ["Scientist", "Data Scientist"],
            "BI Analyst": ["BI", "Analyst"],
            "AI Manager": ["AI", "Manager"],
            "AI Project Manager": ["AI", "Manager"],
            "Prompt Engineer": ["Engineer", "AI"],
            "Machine Learning Engineer": ["Engineer", "ML", "Machine Learning"],
            "Data Engineer": ["Engineer", "Data"]
        }

        for role in CONFIG["ROLES"]:
            keywords = TREND_KEYWORDS_MAP.get(role, [role])
            if role not in keywords:
                keywords.insert(0, role)

            for country in CONFIG["COUNTRIES"]:
                print(f"  [TRENDS] Fetching {role} in {country.upper()}...")
                
                success = False
                for kw in keywords:
                    if kw != role:
                        print(f"    [~] Trying alternative keyword: '{kw}'")
                    
                    history_data = adzuna.fetch_salary_history(country=country, query=kw)
                    if history_data:
                        self.db.save_salary_history(country, role, history_data)
                        print(f"    [+] Saved history for {role} using '{kw}' ({len(history_data)} months)")
                        success = True
                        break 
                
                if not success:
                    print(f"    [-] No history data found for {role} in {country.upper()} after trying all keywords.")
                
                time.sleep(1) # Quota friendly

        print("\n=== TRENDS COLLECTION FINISHED ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--trends", action="store_true", help="Run only salary trends collection")
    parser.add_argument("--scrape", action="store_true", help="Run only vacancy scraping")
    parser.add_argument("--enrich", action="store_true", help="Run only description enrichment")
    parser.add_argument("--skills", action="store_true", help="Run only skill extraction")
    parser.add_argument("--reset", action="store_true", help="Clear all data from the database before starting")
    args = parser.parse_args()
    
    pipeline = Pipeline(is_test=args.test)
    
    if args.reset:
        confirm = input("[?] Are you sure you want to clear ALL data from the database? (y/n): ")
        if confirm.lower() == 'y':
            pipeline.db.clear_all_data()
        else:
            print("[!] Reset cancelled.")
            exit()

    if args.trends:
        pipeline.run_salary_trends()
    elif args.scrape or args.enrich or args.skills:
        # Run specific components
        pipeline.run(scrape=args.scrape, enrich=args.enrich, skills=args.skills)
    else:
        # Default full run
        pipeline.run()

    # Final quota usage report
    adzuna = pipeline.scrapers.get("adzuna")
    if adzuna:
        status = adzuna.usage.get_status()
        print("\n=== ADZUNA API QUOTA USAGE ===")
        print(f"  Minute:  {status['minute']}/{adzuna.LIMITS['minute']}")
        print(f"  Daily:   {status['daily']}/{adzuna.LIMITS['daily']}")
        print(f"  Weekly:  {status['weekly']}/{adzuna.LIMITS['weekly']}")
        print(f"  Monthly: {status['monthly']}/{adzuna.LIMITS['monthly']}")
        print("==============================\n")
