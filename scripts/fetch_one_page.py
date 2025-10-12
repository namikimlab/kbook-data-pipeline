mkdir -p scripts
cat > scripts/fetch_one_page.py <<'PY'
import os, json, time, random, requests, psycopg
from dotenv import load_dotenv

load_dotenv()
NL_CERT_KEY = os.getenv("NL_CERT_KEY")
DB_URL = os.getenv("SUPABASE_DB_URL")

def fetch_page(page_no=1, page_size=5, max_retries=5):
    url = "https://www.nl.go.kr/seoji/SearchApi.do"
    params = {
        "cert_key": NL_CERT_KEY,
        "result_style": "json",
        "page_no": page_no,
        "page_size": page_size,
        "sort": "INPUT_DATE",
        "order_by": "DESC",
    }
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=(5, 45))
            r.raise_for_status()
            data = r.json()
            return data.get("docs", [])
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(3 * (attempt + 1))
    return []

def insert_records(records, page_no):
    if not records:
        print("No records to insert.")
        return 0
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            for rec in records:
                cur.execute("""
                    INSERT INTO public.raw_nl_books (page_no, source_record)
                    VALUES (%s, %s)
                    ON CONFLICT (rec_hash) DO NOTHING;
                """, (page_no, json.dumps(rec)))
        conn.commit()
    print(f"✅ Inserted {len(records)}")
    return len(records)

if __name__ == "__main__":
    page_no = 1
    docs = fetch_page(page_no, page_size=5)
    insert_records(docs, page_no)
PY
