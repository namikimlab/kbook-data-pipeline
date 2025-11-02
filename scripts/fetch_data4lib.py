import os, sys, html, requests, xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

isbn = sys.argv[1]
url = "http://data4library.kr/api/srchDtlList"
r = requests.get(url, params={
    "authKey": os.getenv("D4L_CERT_KEY"),
    "isbn13": isbn,
    "loaninfoYN": "N"
})

root = ET.fromstring(r.text)
for elem in root.iter():
    if elem.text and elem.text.strip():
        print(f"{elem.tag:20}: {elem.text.strip()}")