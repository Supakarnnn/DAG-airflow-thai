"""FRED extract — ดึง series เดียวคืน DataFrame (obs_date, value). ไม่แตะ DB."""
import os
from dotenv import load_dotenv
import pandas as pd
from fredapi import Fred

load_dotenv()

def fetch_series(series_id: str, start: str | None = None) -> pd.DataFrame:
    fred = Fred(api_key=os.environ["FRED_API_KEY"])
    s = fred.get_series(series_id, observation_start=start)
    df = s.rename("value").rename_axis("obs_date").reset_index()
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    return df.dropna(subset=["value"]).reset_index(drop=True)  # FRED ใส่ NaN วันหยุด


if __name__ == "__main__":  # self-check: FRED_API_KEY=... python -m src.extract.fred
    out = fetch_series("DEXTHUS", start="2024-01-01")
    assert list(out.columns) == ["obs_date", "value"], "คอลัมน์ผิด"
    assert out["value"].notna().all(), "ยังมี NaN หลุดมา"
    print(f"OK: {len(out)} แถว, {out.obs_date.min()} → {out.obs_date.max()}, ล่าสุด={out.value.iloc[-1]}")
