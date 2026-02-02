import re
import hashlib

def clean_text(text):
    if not text:
        return ""
    # Приводим к нижнему регистру
    text = str(text).lower()
    # Удаляем (m/w/d), (f/m/d), (gn) и прочие гендерные приписки
    text = re.sub(r'[\(\[/]*(m/w/d|f/m/d|w/m/d|m/f/d|gn|m/w/x|x/m/w)[\)\]/]*', '', text)
    
    # Удаляем правовые формы компаний (GmbH, AG, SE и т.д.)
    text = re.sub(r'\b(gmbh|ag|se|kg|limited|ltd|inc|llc|gbr|co\.? kg|kgaa)\b', '', text)
    
    # Очищаем от спецсимволов и лишних пробелов, но оставляем буквы и цифры
    text = re.sub(r'[^a-z0-9а-яё ]', ' ', text)
    text = " ".join(text.split())
    return text.strip()

def normalize_location(loc):
    if not loc or loc.lower() in ["deutschland", "germany", "remote", "nationwide", "home office", "homeoffice"]:
        return "Remote/Deutschland"
    
    # Если формат "City, State", берем City
    if "," in loc:
        loc = loc.split(",")[0]
    
    # Очистка от почтовых индексов и лишних слов (типа "Berlin city", "Frankfurt am Main (Hessen)")
    loc = re.sub(r'\d{5}', '', loc)
    loc = loc.replace(" - ", " ").replace("/", " ")
    
    # Маппинг для склейки разных названий одного города
    mapping = {
        'frankfurt': 'Frankfurt am Main',
        'ffm': 'Frankfurt am Main',
        'münchen': 'München',
        'munich': 'München',
        'köln': 'Köln',
        'cologne': 'Köln',
        'nürnberg': 'Nürnberg',
        'nuremberg': 'Nürnberg',
        'düsseldorf': 'Düsseldorf',
        'hannover': 'Hannover',
        'zürich': 'Zürich',
        'zurich': 'Zürich',
        'wien': 'Wien',
        'vienna': 'Wien'
    }
    
    loc_clean = loc.lower().strip()
    # Ищем точное вхождение названия города в строку
    for key, val in mapping.items():
        if key in loc_clean:
            return val
            
    return loc.strip().title()

def get_job_signature(title, company, location):
    """
    Создает уникальный хеш на основе названия вакансии, компании и города.
    Это помогает найти одну и ту же вакансию на разных сайтах.
    """
    t = clean_text(title)
    c = clean_text(company)
    l = clean_text(location)
    
    # Создаем строку-сигнатуру
    sig_string = f"{t}|{c}|{l}"
    return hashlib.md5(sig_string.encode()).hexdigest()

if __name__ == "__main__":
    # Тест
    print(f"Signature 1: {get_job_signature('Data Analyst (m/w/d)', 'Google GmbH', 'Berlin')}")
    print(f"Signature 2: {get_job_signature('Data Analyst', 'Google', 'Berlin city')}")
