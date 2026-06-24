"""BOT extract — ดึงดอกเบี้ยนโยบายไทย 'ปัจจุบัน' (ค่าเดียว + วันมีผล + news). ไม่แตะ DB."""
import os
from datetime import datetime

import requests

URL = "https://gateway.api.bot.or.th/PolicyRate/v3/policy_rate"


def fetch_policy_rate() -> dict:
    """คืน dict: obs_date (วันมีผล), value, news_th, news_en, raw (payload เต็มไว้ลง bronze)."""
    r = requests.get(URL, headers={"Authorization": os.environ["BOT_API_KEY"]}, timeout=30)
    r.raise_for_status()
    res = r.json()["result"]
    eff = datetime.strptime(res["effective_datetime"], "%Y-%m-%d %H:%M:%S").date()
    return {
        "obs_date": str(eff),
        "value": float(res["data"]),
        "news_th": res.get("news_text_th"),
        "news_en": res.get("news_text_en"),
        "raw": res,
    }


if __name__ == "__main__":  # self-check: uv run -m src.extract.bot  (ต้องมี BOT_API_KEY ใน env)
    from dotenv import load_dotenv
    load_dotenv()
    out = fetch_policy_rate()
    assert out["value"] > 0, "rate ต้องเป็นบวก"
    assert len(out["obs_date"]) == 10, "obs_date ควรเป็น YYYY-MM-DD"
    print(f"OK: {out['obs_date']} = {out['value']}%")
