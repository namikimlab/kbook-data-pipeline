
import os, sys, json, requests
from dotenv import load_dotenv

load_dotenv()
KAKAO_KEY = os.getenv("KAKAO_REST_API")

if not KAKAO_KEY:
    print("❌ Missing KAKAO_REST_API in .env")
    sys.exit(1)

isbn = sys.argv[1]
url = "https://dapi.kakao.com/v3/search/book"
headers = {"Authorization": f"KakaoAK {KAKAO_KEY}"}
params = {"target": "isbn", "query": isbn}

res = requests.get(url, headers=headers, params=params)
try:
    data = res.json()
except Exception as e:
    print("❌ Failed to parse JSON:", e)
    print("Response text:\n", res.text)
    sys.exit(1)

print(json.dumps(data, indent=2, ensure_ascii=False))
