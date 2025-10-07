#!/usr/bin/env python3
"""
Convert JSON-LD with top-level @graph into NDJSON
so it can be streamed safely (no IncompleteJSONError).
"""

import json
import argparse
from pathlib import Path
import ijson

def convert(infile: Path, outfile: Path) -> int:
    count = 0
    with infile.open("rb") as f, outfile.open("w", encoding="utf-8") as out:
        # Stream each item in @graph
        for item in ijson.items(f, "@graph.item"):
            out.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
    return count

def main():
    ap = argparse.ArgumentParser(description="Convert JSON-LD @graph to NDJSON")
    ap.add_argument("--infile", required=True, help="Path to raw JSON-LD file")
    ap.add_argument("--outfile", required=True, help="Path to output NDJSON file")
    args = ap.parse_args()

    infile = Path(args.infile)
    outfile = Path(args.outfile)

    n = convert(infile, outfile)
    print(f"✅ Wrote {n} records to {outfile}")

if __name__ == "__main__":
    main()
