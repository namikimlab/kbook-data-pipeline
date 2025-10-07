# scripts/load_books.py
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from psycopg.types.json import Json


BOOK_FILE = Path("data/book_sample.json")
YEAR_RE = re.compile(r"\b(1\d{3}|20\d{2})\b")

def extract_list(v: Any) -> Optional[List[str]]:
    """Return v as a list[str]: list -> as-is; str -> [str]; dict -> label/name/@id/json; else None."""
    def _one(x: Any) -> Optional[str]:
        if x is None:
            return None
        if isinstance(x, dict):
            # prefer human label, then name, then @id, else JSON string
            return (x.get("label") or x.get("name") or x.get("@id") or json.dumps(x, ensure_ascii=False)).strip()
        return str(x).strip()

    if v is None:
        return None
    if isinstance(v, list):
        out = [s for s in (_one(x) for x in v) if s]
        return out or None
    s = _one(v)
    return [s] if s else None

def parse_pub_year(item: Dict[str, Any]) -> Optional[int]:
    """
    Return a clean pub_year if 1000–2099.
    If another 4-digit number is found, leave pub_year None
    and let ingest keep it in 'extra'.
    """
    y = item.get("issuedYear")
    if isinstance(y, str) and y.isdigit() and len(y) == 4:
        if YEAR_RE.fullmatch(y):
            return int(y)
        else:
            return None

    issued = item.get("issued")
    if isinstance(issued, str):
        m = YEAR_RE.search(issued)
        if m:
            return int(m.group(1))
        else:
            # found some 4-digit year but not in 1000–2099
            # don’t normalize to pub_year, so it will stay in extra
            return None

    return None

def is_korean_book(item: dict) -> bool:
    """Return True if any language field contains 'kor' (case-insensitive)."""
    for key in ("language", "bibframe:language", "dcterms:language"):
        val = item.get(key)
        if val is None:
            continue
        # normalize to a list of strings
        vals = val if isinstance(val, list) else [val]
        # accept if any entry contains 'kor'
        if any("kor" in str(x).lower() for x in vals):
            return True
    return False

def to_row(item: Dict[str, Any]) -> Dict[str, Any]:
    mapped_keys = {
        "@id","title","remainderOfTitle","creator","subject",
        "publisher","issuedYear","issued","titleOfSeries","volumeOfSeries","volume","isbn"
    }
    return {
        "source_id": item.get("@id"),
        "title": item.get("title"),
        "subtitle": item.get("remainderOfTitle"),
        "authors": extract_list(item.get("creator")),   # only 'creator'
        "subjects": extract_list(item.get("subject")),   # reuse the same helper
        "publisher": item.get("publisher"),
        "pub_year": parse_pub_year(item),
        "series": item.get("titleOfSeries"),
        "volume": item.get("volumeOfSeries") or item.get("volume"),
        "isbn13": item.get("isbn"),
        "extra": Json({k: v for k, v in item.items() if k not in mapped_keys}),
    }

def load_books(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if not is_korean_book(item):
                continue
            row = to_row(item)
            if not row.get("source_id"):
                continue
            yield row


