"""Step 1: ยิง BOT Policy Rate ดู raw JSON ก่อน. รัน: uv run try_bot.py
เป้าหมาย: ดูชื่อ field + param ที่ถูก ก่อนเขียน extract client จริง"""
import json
import os
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

URL = "https://gateway.api.bot.or.th/PolicyRate/v3/policy_rate"
headers = {"Authorization": os.environ["BOT_API_KEY"]}
params = {
    "start_period": str(date.today() - timedelta(days=120)),
    "end_period": str(date.today()),
}

r = requests.get(URL, headers=headers, params=params, timeout=30)
print("HTTP", r.status_code)
try:
    print(json.dumps(r.json(), ensure_ascii=False, indent=2)[:2000])
except Exception:
    print(r.text[:2000])
