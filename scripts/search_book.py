# scripts/search_book.py
import os, json, ijson

INPUT_FILE = os.path.join("data", "book.json")
OUTPUT_FILE = "found_book.txt"
TARGET_TITLE = "가면산장 살인사건"
TARGET_ISBN = "9788934942467"

found = None

with open(INPUT_FILE, "rb") as f:
    for book in ijson.items(f, "@graph.item", multiple_values=True):
        if book.get("title") == TARGET_TITLE:
        #if book.get("title") == TARGET_ISBN:
            found = book
            break  # stop at the first match

if found:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        json.dump(found, out, ensure_ascii=False, indent=2)
    print(f"✅ Found book titled '{TARGET_TITLE}', saved to {OUTPUT_FILE}")
else:
    print(f"❌ No book with title '{TARGET_TITLE}' found.")
