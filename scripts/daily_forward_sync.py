import os, json, time, random, requests, psycopg
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
NL_CERT_KEY = os.getenv("NL_CERT_KEY")
DB_URL      = os.getenv("SUPABASE_DB_URL")

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

def yyyymmdd(s: str):
    try: return datetime.strptime(s, "%Y%m%d").date()
    except: return None

def get_last_synced(conn):
    with conn.cursor() as cur:
        cur.execute("select last_input_date_synced from public.sync_state where job_name='nl_forward'")
        row = cur.fetchone()
        return row[0] if row else None

def set_last_synced(conn, new_date):
    with conn.cursor() as cur:
        cur.execute("""
            update public.sync_state
               set last_input_date_synced=%s, last_run_at=now()
             where job_name='nl_forward'
        """, (new_date,))
    conn.commit()

def fetch_page(page_no=1, page_size=10, max_retries=5):
    url = "https://www.nl.go.kr/seoji/SearchApi.do"
    params = {
        "cert_key": NL_CERT_KEY, "result_style": "json",
        "page_no": page_no, "page_size": page_size,
        "sort": "INPUT_DATE", "order_by": "DESC",
    }
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=(5, 45))
            r.raise_for_status()
            j = r.json()
            return j.get("docs", [])
        except requests.RequestException as e:
            print(f"[page {page_no}] attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(3 * (attempt + 1))
    return []

def insert_records(conn, records, page_no):
    if not records: return 0
    with conn.cursor() as cur:
        for rec in records:
            cur.execute("""
                insert into kbooks_raw.raw_nl_books (page_no, source_record)
                values (%s, %s)
                on conflict (rec_hash) do nothing
            """, (page_no, json.dumps(rec)))
    conn.commit()
    return len(records)

if __name__ == "__main__":
    PAGE_SIZE  = 10
    MAX_PAGES  = 100  # safety cap per run
    total_ins  = 0
    max_input_seen = None

    with psycopg.connect(DB_URL) as conn:
        last_synced = get_last_synced(conn)  # may be None on first real run
        print(f"last_input_date_synced = {last_synced}")

        page_no = 1
        while page_no <= MAX_PAGES:
            docs = fetch_page(page_no, PAGE_SIZE)
            if not docs:
                print("No docs returned. Stopping.")
                break

            # find oldest & newest INPUT_DATE on this page
            input_dates = [yyyymmdd(d.get("INPUT_DATE","")) for d in docs if d.get("INPUT_DATE")]
            # if page has no INPUT_DATE values, treat as insert-only and continue
            oldest = min(input_dates) if input_dates else None
            newest = max(input_dates) if input_dates else None

            # stop condition: page’s oldest <= last_synced
            if last_synced and oldest and oldest <= last_synced:
                print(f"Stop: oldest INPUT_DATE on page ({oldest}) <= last_synced ({last_synced})")
                # Still insert only the records that are newer than last_synced
                fresh = [d for d in docs if yyyymmdd(d.get("INPUT_DATE","")) and yyyymmdd(d["INPUT_DATE"]) > last_synced]
                ins = insert_records(conn, fresh, page_no)
                total_ins += ins
                if fresh:
                    max_input_seen = max([yyyymmdd(d["INPUT_DATE"]) for d in fresh if d.get("INPUT_DATE")])
                break

            # normal insert for full page
            ins = insert_records(conn, docs, page_no)
            total_ins += ins
            if newest and (max_input_seen is None or newest > max_input_seen):
                max_input_seen = newest

            page_no += 1
            time.sleep(random.uniform(2.5, 5.0))  # polite pacing

        # update cursor to newest INPUT_DATE we actually saw this run
        if max_input_seen and (not last_synced or max_input_seen > last_synced):
            set_last_synced(conn, max_input_seen)
            print(f"Updated last_input_date_synced -> {max_input_seen}")
        print(f"Done. Inserted {total_ins} rows.")
