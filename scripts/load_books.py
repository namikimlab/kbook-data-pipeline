import os, re, json, ijson, yaml, psycopg
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")
BATCH_SIZE = 500

# --- helpers --------------------------------------------------------------

def clean_isbn(isbn: str | None) -> str | None:
    if not isbn:
        return None
    digits = re.sub(r"[^0-9Xx]", "", isbn)
    return digits if digits else None

def extract_year(raw):
    if not raw:
        return None
    m = re.search(r"(19|20)\d{2}", raw)
    return int(m.group()) if m else None

# --- mapper ---------------------------------------------------------------

def map_node(node: dict, mapping: dict) -> dict:
    """Map JSON-LD node to flat dict for books table."""
    out = {}
    cols = mapping["columns"]

    for col, key in cols.items():
        val = None
        if isinstance(key, list):
            for k in key:
                if k in node:
                    val = node[k]
                    break
        else:
            val = node.get(key)

        # Normalize some
        if col == "isbn13":
            val = clean_isbn(val)
        elif col == "pub_year":
            val = extract_year(val)
        elif col in ("authors", "subjects"):
            val = val if isinstance(val, list) else [val] if val else []
        out[col] = val

    # Store all leftovers in extra
    mapped_keys = {k for v in cols.values() for k in ([v] if isinstance(v, str) else v)}
    extra = {k: v for k, v in node.items() if k not in mapped_keys}
    out["extra"] = json.dumps(extra)
    return out

# --- loader ---------------------------------------------------------------

def upsert_batch(conn, rows):
    if not rows:
        return
    cols = rows[0].keys()
    placeholders = ", ".join(f"%({c})s" for c in cols)
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "book_id")
    sql = f"""
        INSERT INTO books ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (source_id) DO UPDATE SET {updates};
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

# --- main ----------------------------------------------------------------

def main():
    mapping = yaml.safe_load(Path("config/mapping.yml").read_text())
    path = Path("data/book.json")

    with psycopg.connect(DB_URL) as conn:
        rows = []
        for node in ijson.items(path.open("r", encoding="utf-8"), "@graph.item"):
            rows.append(map_node(node, mapping))
            if len(rows) >= BATCH_SIZE:
                upsert_batch(conn, rows)
                print(f"Inserted {len(rows)} rows…")
                rows.clear()
        if rows:
            upsert_batch(conn, rows)
            print(f"Inserted final {len(rows)} rows.")

if __name__ == "__main__":
    main()
