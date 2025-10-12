import os, json, requests, psycopg, time
from dotenv import load_dotenv

load_dotenv()

NL_CERT_KEY = os.getenv("NL_CERT_KEY")
DB_URL = os.getenv("SUPABASE_DB_URL")

def fetch_page(page_no=1, page_size=10, max_retries=5):
    url = "https://www.nl.go.kr/seoji/SearchApi.do"
    params = {
        "cert_key": NL_CERT_KEY,
        "result_style": "json",
        "page_no": page_no,
        "page_size": page_size,
        "sort": "INPUT_DATE",
        "order_by": "DESC",
    }

    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=(10, 15))  # (connect, read)
            r.raise_for_status()
            data = r.json()
            return data.get("docs", [])
        except requests.exceptions.Timeout:
            print(f"⏱️ Timeout on attempt {attempt+1}/{max_retries}, retrying...")
            time.sleep(2 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error: {e}")
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("Failed to fetch page after retries")

def insert_records(records):
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            for rec in records:
                cur.execute("""
                    INSERT INTO public.raw_nl_books (page_no, source_record)
                    VALUES (%s, %s)
                    ON CONFLICT (rec_hash) DO NOTHING;
                """, (1, json.dumps(rec)))
        conn.commit()
    print(f"Inserted {len(records)} records")

if __name__ == "__main__":
    docs = fetch_page(page_no=1, page_size=5)
    print(f"Fetched {len(docs)} docs")
    insert_records(docs)
