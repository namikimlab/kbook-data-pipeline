# scripts/fetch_pages_month.py
import os, json, time, random, requests, psycopg, math
from datetime import date
from dotenv import load_dotenv
load_dotenv()

NL_CERT_KEY = os.getenv("NL_CERT_KEY")
DB_URL = os.getenv("SUPABASE_DB_URL")

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def month_bounds(year: int, month: int):
    start = date(year, month, 1)
    # simple month-end calc
    if month == 12:
        end = date(year + 1, 1, 1)  # exclusive helper
    else:
        end = date(year, month + 1, 1)
    # inclusive end (last day of month)
    end_inclusive = end.fromordinal(end.toordinal() - 1)
    return yyyymmdd(start), yyyymmdd(end_inclusive)

def fetch_page(page_no=1, page_size=50, max_retries=5,
               start_publish_date=None, end_publish_date=None,
               sort="INPUT_DATE", order_by="ASC"):
    """
    Note: start/end are for PUBLISH_PREDATE; not INPUT_DATE.
    """
    url = "https://www.nl.go.kr/seoji/SearchApi.do"
    params = {
        "cert_key": NL_CERT_KEY,
        "result_style": "json",
        "page_no": page_no,
        "page_size": page_size,
        "sort": sort,
        "order_by": order_by,
    }
    if start_publish_date:
        params["start_publish_date"] = start_publish_date
    if end_publish_date:
        params["end_publish_date"] = end_publish_date

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=(5, 45))
            r.raise_for_status()
            payload = r.json() or {}
            docs = payload.get("docs", []) or []
            total_count = payload.get("TOTAL_COUNT")  # may be None
            return docs, total_count
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(3 * (attempt + 1))
    return [], None

def insert_records(records, page_no):
    if not records:
        return 0
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        for rec in records:
            cur.execute("""
                INSERT INTO public.raw_nl_books (page_no, source_record)
                VALUES (%s, %s)
                ON CONFLICT (rec_hash) DO NOTHING;
            """, (page_no, json.dumps(rec)))
        conn.commit()
    print(f"✅ Inserted {len(records)} from page {page_no}")
    return len(records)

def backfill_month(year: int, month: int, page_size: int = 50, pages_per_run: int = None):
    """
    Fetch all pages for a given publish month (bounded by start/end_publish_date).
    pages_per_run: if provided, limits how many pages we attempt this invocation (optional).
    """
    start_str, end_str = month_bounds(year, month)
    print(f"▶ Backfill month {year}-{month:02d} (publish window {start_str}..{end_str})")
    t0 = time.time()

    page_no = 1
    total_inserted = 0
    total_count_snapshot = None
    pages_done = 0

    while True:
        if pages_per_run and pages_done >= pages_per_run:
            print("Reached pages_per_run cap; stopping this invocation.")
            break

        docs, total_count = fetch_page(
            page_no=page_no,
            page_size=page_size,
            start_publish_date=start_str,
            end_publish_date=end_str,
            sort="INPUT_DATE",     # keep INPUT_DATE for stable crawl inside the slice
            order_by="ASC"
        )
        if total_count is not None and total_count_snapshot is None:
            total_count_snapshot = total_count

        if not docs:
            print("No more docs for this month slice. Stopping.")
            break

        total_inserted += insert_records(docs, page_no)
        pages_done += 1
        page_no += 1

        # polite pacing
        time.sleep(random.uniform(2.5, 5.0))

        # optional early stop if last page smaller than page_size (likely exhausted)
        if len(docs) < page_size:
            print("Last page smaller than page_size — month likely exhausted.")
            break

    elapsed = time.time() - t0
    print(f"⏱ Elapsed: {elapsed:.1f}s | Inserted this run: {total_inserted}" +
          (f" | TOTAL_COUNT snapshot: {total_count_snapshot}" if total_count_snapshot is not None else ""))

if __name__ == "__main__":
    # For this timing experiment: run exactly 2025-01, full month
    backfill_month(2025, 1, page_size=50, pages_per_run=None)
