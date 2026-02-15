import sys
import os
import streamlit as st

# Simulate the path streamlit would have
sys.path.append(os.path.abspath("dashboard"))

try:
    import main_dashboard
    print("Successfully imported main_dashboard")
    print(f"DB_PATH defined as: {main_dashboard.DB_PATH}")
    if os.path.exists(main_dashboard.DB_PATH):
        print("Database exists at path.")
    else:
        print(f"Database NOT found at: {main_dashboard.DB_PATH}")
except Exception as e:
    print(f"Import error: {e}")
