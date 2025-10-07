# test_db.py using Session pooler 
import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

conn_str = os.getenv("POOLER_CONN")
if not conn_str:
    raise SystemExit("❌ Missing POOLER_CONN in .env")

# Sanity check (don’t print password)
print("Using Session Pooler host:", conn_str.split("@")[-1])

try:
    with psycopg.connect(conn_str + "?sslmode=require") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT now(), current_database(), version()")
            row = cur.fetchone()
            print("✅ Query result:", row)

    print("✅ Connection successful and closed properly.")

except Exception as e:
    print("❌ Failed to connect:", e)
