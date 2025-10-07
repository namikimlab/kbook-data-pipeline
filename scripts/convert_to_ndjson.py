# scripts/convert_to_ndjson.py
#!/usr/bin/env python3
import json
import argparse
from pathlib import Path

def convert(infile: Path, outfile: Path, chunk_size: int = 1_000_000) -> int:
    dec = json.JSONDecoder()
    buf = ""
    count = 0
    with infile.open("r", encoding="utf-8") as f, outfile.open("w", encoding="utf-8") as out:
        while True:
            chunk = f.read(chunk_size)
            if not chunk and not buf:
                break
            buf += chunk

            # Try to peel off as many complete JSON objects as exist in buf
            start = 0
            while True:
                # Skip leading whitespace
                while start < len(buf) and buf[start].isspace():
                    start += 1
                if start >= len(buf):
                    buf = ""
                    break
                try:
                    obj, end = dec.raw_decode(buf, idx=start)
                except json.JSONDecodeError:
                    # Need more data
                    buf = buf[start:]  # drop leading whitespace
                    break

                # We decoded one top-level object [start:end]
                # If it has @graph, emit each item; else, emit the object itself (rare)
                graph = obj.get("@graph")
                if isinstance(graph, list):
                    for item in graph:
                        out.write(json.dumps(item, ensure_ascii=False) + "\n")
                        count += 1
                else:
                    out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    count += 1

                # Move on to any remaining concatenated JSON in the buffer
                start = end
                if start >= len(buf):
                    buf = ""
                    break

        return count

def main():
    ap = argparse.ArgumentParser(description="Convert (possibly concatenated) JSON-LD with @graph → NDJSON.")
    ap.add_argument("--infile", required=True)
    ap.add_argument("--outfile", required=True)
    args = ap.parse_args()
    n = convert(Path(args.infile), Path(args.outfile))
    print(f"✅ Wrote {n} records to {args.outfile}")

if __name__ == "__main__":
    main()
