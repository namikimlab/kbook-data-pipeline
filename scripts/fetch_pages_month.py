import os, json, time, random, requests, psycopg, argparse
from datetime import date
from dotenv import load_dotenv

load_dotenv()

NL_CERT_KEY = os.getenv("NL_CERT_KEY")
DB_URL = os.getenv("SUPABASE_DB_URL")

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def month_bounds(year: int, month: int):
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    end_inclusive = end.fromordinal(end.toordinal() - 1)
    return yyyymmdd(start), yyyymmdd(end_inclusive)

def fetch_page(page_no=1, page_size=50, max_retries=5,
               start_publish_date=None, end_publish_date=None,
               sort="INPUT_DATE", order_by="ASC"):
    """
    Returns: (docs, total_count, ok)
      - docs: list
      - total_count: int | None
      - ok: bool (False iff we exhausted retries / network error)
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
            # longer read timeout to avoid false "end of data"
            r = requests.get(url, params=params, headers=headers, timeout=(5, 90))
            r.raise_for_status()
            payload = r.json() or {}
            docs = payload.get("docs", []) or []
            total_count = payload.get("TOTAL_COUNT")
            return docs, total_count, True
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}/{max_retries} for page {page_no}: {e}")
            time.sleep(3 * (attempt + 1))
    # failed all retries: signal error distinctly
    return [], None, False

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

def backfill_month(year: int, month: int, page_size: int = 50,
                   start_page: int = 1, pages_per_run: int = None):
    start_str, end_str = month_bounds(year, month)
    print(f"▶ Backfill month {year}-{month:02d} (publish window {start_str}..{end_str})")
    t0 = time.time()

    page_no = max(1, start_page)
    total_inserted = 0
    total_count_snapshot = None
    pages_done = 0

    while True:
        if pages_per_run and pages_done >= pages_per_run:
            print("Reached pages_per_run cap; stopping this invocation.")
            break

        docs, total_count, ok = fetch_page(
            page_no=page_no,
            page_size=page_size,
            start_publish_date=start_str,
            end_publish_date=end_str,
            sort="INPUT_DATE",
            order_by="ASC"
        )

        # snapshot only on first successful page
        if ok and total_count is not None and total_count_snapshot is None:
            total_count_snapshot = total_count

        if not ok:
            # Network/API error: DO NOT advance page_no. Pause so you can resume this same page.
            print(f"⚠️  Pausing on page {page_no} due to network error. "
                  f"Resume with --start-page {page_no}")
            break

        if not docs:
            # True end-of-data only when ok==True and zero docs returned.
            print("No more docs for this month slice. Stopping.")
            break

        total_inserted += insert_records(docs, page_no)
        pages_done += 1
        page_no += 1

        # polite pacing
        time.sleep(random.uniform(2.5, 5.0))

        # Likely exhausted when last page smaller than page_size (ok==True)
        if len(docs) < page_size:
            print("Last page smaller than page_size — month likely exhausted.")
            break

    elapsed = time.time() - t0
    print(f"⏱ Elapsed: {elapsed:.1f}s | Inserted this run: {total_inserted}"
          + (f" | TOTAL_COUNT snapshot: {total_count_snapshot}" if total_count_snapshot is not None else ""))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--pages-per-run", type=int, default=None)
    args = parser.parse_args()

    backfill_month(
        year=args.year,
        month=args.month,
        page_size=args.page_size,
        start_page=args.start_page,
        pages_per_run=args.pages_per_run
    )
