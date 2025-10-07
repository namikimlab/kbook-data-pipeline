# scripts/ingest_books.py
import os
import time
import json
import argparse
import logging
import math
from pathlib import Path
from typing import Iterable, Dict, Any, List, Tuple

import psycopg
from dotenv import load_dotenv

from load_books import load_books

def setup_logging(logfile: str = "ingest.log"):
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(console)

def ensure_sslmode_require(conn_str: str) -> str:
    if "sslmode=" in conn_str:
        return conn_str
    sep = "&" if "?" in conn_str else "?"
    return conn_str + f"{sep}sslmode=require"

def chunked(stream: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for row in stream:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

UPSERT_SQL = """
insert into public.books (
  source_id, title, subtitle, authors, subjects, publisher,
  pub_year, series, volume, isbn13, extra
) values (
  %(source_id)s, %(title)s, %(subtitle)s, %(authors)s, %(subjects)s, %(publisher)s,
  %(pub_year)s, %(series)s, %(volume)s, %(isbn13)s, %(extra)s
)
on conflict (source_id) do update set
  title      = excluded.title,
  subtitle   = excluded.subtitle,
  authors    = excluded.authors,
  subjects   = excluded.subjects,
  publisher  = excluded.publisher,
  pub_year   = excluded.pub_year,
  series     = excluded.series,
  volume     = excluded.volume,
  isbn13     = excluded.isbn13,
  extra      = excluded.extra,
  updated_at = now();
"""

def write_failed_rows(rows: List[Dict[str, Any]], path: Path) -> int:
    """Append rows to a JSONL failures file. Returns number written."""
    with path.open("a", encoding="utf-8") as out:
        for r in rows:
            out.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(rows)

def upsert_stream(conn: psycopg.Connection,
                  rows_stream: Iterable[Dict[str, Any]],
                  batch_size: int,
                  failed_path: Path,
                  total_batches: input, 
                  max_retries: int = 3,
                  start_time = time.time()) -> Tuple[int, int, int]:
    """
    Returns (total_seen, ok_ingested, failed_rows).
    - Commits per batch.
    - Retries transient failures.
    - On permanent failure (or bad data), dumps entire batch to failures file.
    """
    start_time = time.time()
    total_seen = 0
    ok_ingested = 0
    failed_rows = 0

    with conn.cursor() as cur:
        for i, batch in enumerate(chunked(rows_stream, batch_size), start=1):
            total_seen += len(batch)

            # quick sanity: pub_year int or None
            bad_year = next((r for r in batch
                             if r.get("pub_year") is not None and not isinstance(r["pub_year"], int)), None)
            if bad_year:
                logging.error(f"Batch {i}: non-integer pub_year example={bad_year.get('pub_year')}; dumping batch.")
                failed_rows += write_failed_rows(batch, failed_path)
                continue

            attempt = 0
            while True:
                try:
                    cur.executemany(UPSERT_SQL, batch)
                    conn.commit()
                    ok_ingested += len(batch)
                    # add logging
                    elapsed = time.time() - start_time
                    eta = (elapsed / i) * (total_batches - i)
                    logging.info(
                        f"Batch {i}/{total_batches}: "
                        f"ok={ok_ingested}, total_seen={total_seen}, "
                        f"elapsed={elapsed/60:.1f}m ETA={eta/60:.1f}m"
                    )
                    break
                except Exception as e:
                    conn.rollback()
                    attempt += 1
                    if attempt > max_retries:
                        logging.error(f"Batch {i}: permanent failure after {max_retries} retries: {e}. Dumping rows.")
                        failed_rows += write_failed_rows(batch, failed_path)
                        break
                    sleep_for = 0.4 * (2 ** (attempt - 1))
                    logging.warning(f"Batch {i}: attempt {attempt}/{max_retries} failed: {e}. Retrying in {sleep_for:.1f}s")
                    time.sleep(sleep_for)

    return total_seen, ok_ingested, failed_rows

def main() -> int:
    p = argparse.ArgumentParser(description="Stream-ingest JSON-LD @graph into Supabase (upsert).")
    p.add_argument("--file", default="data/book.json", help="Path to JSON-LD file")
    p.add_argument("--batch", type=int, default=1000, help="Batch size (default 1000)")
    p.add_argument("--logfile", default="ingest.log", help="Log file path")
    p.add_argument("--failed-file", default="failed_rows.jsonl", help="Where to write failed rows (JSONL)")
    p.add_argument("--dry-run", action="store_true", help="Parse only; no DB writes")
    args = p.parse_args()

    setup_logging(args.logfile)

    load_dotenv()
    conn_str = os.getenv("POOLER_CONN")
    if not conn_str:
        logging.error("Missing POOLER_CONN in .env")
        return 2
    conn_str = ensure_sslmode_require(conn_str)
    logging.info("Using Session Pooler host: %s", conn_str.split("@")[-1])

    failed_path = Path(args.failed_file)
    # Truncate failures file at start of each run to avoid mixing results
    failed_path.write_text("", encoding="utf-8")

    rows_stream = load_books(Path(args.file))

    if args.dry_run:
        n = 0
        for n, _ in enumerate(rows_stream, start=1):
            if n % 10000 == 0:
                logging.info("Parsed %d rows...", n)
        logging.info("Dry run complete. Parsed %d rows.", n)
        return 0
    
    # Count rows first
    total_rows = sum(1 for _ in load_books(Path(args.file)))
    total_batches = math.ceil(total_rows / args.batch)
    logging.info(f"Total rows={total_rows}, total_batches={total_batches}")

    try:
        with psycopg.connect(conn_str) as conn:
            conn.execute("set application_name to 'books_ingest_stream';")
            total_seen, ok_ingested, failed_rows = upsert_stream(
                conn, load_books(Path(args.file)), args.batch, failed_path, total_batches
            )
        # Completeness check
        if total_seen == ok_ingested + failed_rows:
            logging.info("✅ Summary: total_seen=%d, ok_ingested=%d, failed_rows=%d (complete)",
                         total_seen, ok_ingested, failed_rows)
        else:
            logging.error("❌ Summary mismatch: total_seen=%d vs ok+failed=%d+%d=%d",
                          total_seen, ok_ingested, failed_rows, ok_ingested + failed_rows)
            logging.error("See %s for failed rows.", failed_path)
        return 0
    except Exception as e:
        logging.error("❌ Ingest failed before batching: %s", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
