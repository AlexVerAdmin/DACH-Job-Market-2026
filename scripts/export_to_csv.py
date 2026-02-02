import sqlite3
import pandas as pd
import os

db_path = "data/jobs_database.sqlite"
export_dir = "exports"

if not os.path.exists(export_dir):
    os.makedirs(export_dir)

def export_table(table_name):
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        file_path = os.path.join(export_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"✅ Таблица '{table_name}' успешно экспортирована в {file_path}")
        print(f"   (Всего строк: {len(df)})")
    except Exception as e:
        print(f"❌ Ошибка при экспорте '{table_name}': {e}")

if __name__ == "__main__":
    print("🚀 Начинаю экспорт таблицы ВАКАНСИЙ в CSV...")
    export_table("vacancies")
    print("\nГотово! Файл 'vacancies.csv' находится в папке 'exports'.")
