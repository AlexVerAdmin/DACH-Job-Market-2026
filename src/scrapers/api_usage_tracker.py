import json
import os
from datetime import datetime

class ApiUsageTracker:
    def __init__(self, file_path="data/api_usage.json"):
        self.file_path = file_path
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        if not os.path.exists(self.file_path):
            self._save_data({
                "minute_hits": 0,
                "daily_hits": 0,
                "weekly_hits": 0,
                "monthly_hits": 0,
                "last_minute": "",
                "last_day": "",
                "last_week": "",
                "last_month": ""
            })

    def _load_data(self):
        default = {
            "minute_hits": 0, "daily_hits": 0, "weekly_hits": 0, "monthly_hits": 0,
            "last_minute": "", "last_day": "", "last_week": "", "last_month": ""
        }
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
                # Обеспечиваем наличие всех ключей при обновлении старого конфига
                for k, v in default.items():
                    if k not in data: data[k] = v
                return data
        except:
            return default

    def _save_data(self, data):
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=4)

    def track_hit(self):
        data = self._load_data()
        now = datetime.now()
        
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        today = now.strftime("%Y-%m-%d")
        current_week = now.strftime("%Y-%W") # Год и номер недели ISO
        current_month = now.strftime("%Y-%m")

        # Сбросы по интервалам
        if data["last_minute"] != current_minute:
            data["minute_hits"] = 0
            data["last_minute"] = current_minute
            
        if data["last_day"] != today:
            data["daily_hits"] = 0
            data["last_day"] = today

        if data["last_week"] != current_week:
            data["weekly_hits"] = 0
            data["last_week"] = current_week

        if data["last_month"] != current_month:
            data["monthly_hits"] = 0
            data["last_month"] = current_month

        data["minute_hits"] += 1
        data["daily_hits"] += 1
        data["weekly_hits"] += 1
        data["monthly_hits"] += 1
        
        self._save_data(data)
        return data

    def get_status(self):
        data = self._load_data()
        now = datetime.now()
        
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        today = now.strftime("%Y-%m-%d")
        current_week = now.strftime("%Y-%W")
        current_month = now.strftime("%Y-%m")

        # Проверяем актуальность каждого счетчика
        res = {
            "minute": data["minute_hits"] if data["last_minute"] == current_minute else 0,
            "daily": data["daily_hits"] if data["last_day"] == today else 0,
            "weekly": data["weekly_hits"] if data["last_week"] == current_week else 0,
            "monthly": data["monthly_hits"] if data["last_month"] == current_month else 0
        }
        return res
