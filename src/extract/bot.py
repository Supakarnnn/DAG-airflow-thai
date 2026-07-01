"""BOT extract — ดอกเบี้ยนโยบาย (PolicyRate API) + series รายเดือนจาก BOT Statistics API. ไม่แตะ DB.
สอง API คนละ product/token: PolicyRate ใช้ BOT_API_KEY, Statistics ใช้ BOT_API_STAT_KEY."""
import os
from datetime import datetime

import pandas as pd
import requests

URL = "https://gateway.api.bot.or.th/PolicyRate/v3/policy_rate"
STAT_OBS_URL = "https://gateway.api.bot.or.th/observations"


def fetch_bot_series(series_code: str, start: str = "2000-01-01", end: str | None = None) -> pd.DataFrame:
    """ดึง 1 series รายเดือนจาก BOT Statistics (/observations) คืน DataFrame (obs_date, value).
    period_start เป็น 'yyyy-mm' → map เป็นวันที่ 1 ของเดือน. generic ใช้ซ้ำได้ทุก series_code (เช่น policy rate)."""
    end = end or datetime.today().strftime("%Y-%m-%d")
    r = requests.get(STAT_OBS_URL, headers={"Authorization": os.environ["BOT_API_STAT_KEY"]},
                     params={"series_code": series_code, "start_period": start, "end_period": end}, timeout=30)
    r.raise_for_status()
    obs = r.json()["result"]["series"][0]["observations"]
    df = pd.DataFrame(obs)
    df["obs_date"] = pd.to_datetime(df["period_start"], format="%Y-%m").dt.date  # yyyy-mm → เดือนที่ 1
    df["value"] = df["value"].astype(float)
    return df[["obs_date", "value"]].dropna().reset_index(drop=True)


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
    print(f"OK policy_rate: {out['obs_date']} = {out['value']}%")

    df = fetch_bot_series("FMRTINTM00296")  # 10Y government bond yield รายเดือน
    assert list(df.columns) == ["obs_date", "value"], "คอลัมน์ผิด"
    assert len(df) > 10 and df["value"].between(0, 15).all(), "yield ผิดรูป/นอกช่วง"
    print(f"OK bond 10Y: {len(df)} แถว, {df.obs_date.min()} → {df.obs_date.max()}, ล่าสุด={df.value.iloc[-1]}")
