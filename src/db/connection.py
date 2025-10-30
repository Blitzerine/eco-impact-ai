import os
import psycopg2
from dotenv import load_dotenv

# 1. figure out absolute path to backend folder (where app.py and .env live)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # this is .../src/db
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))  # go up twice to reach backend

# 2. build full path to .env
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# 3. load .env from that exact file
load_dotenv(ENV_PATH)

def get_db():
    try:
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        name = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        pwd  = os.getenv("DB_PASS")

        print("🔎 USING ENV:")
        print(" host =", host)
        print(" port =", port)
        print(" name =", name)
        print(" user =", user)
        # DON'T print password to console for safety

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=name,
            user=user,
            password=pwd,
            connect_timeout=10
        )
        print("✅ Database connected successfully.")
        return conn
    except Exception as e:
        print("❌ Database connection error:", e)
        return None
