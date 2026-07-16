"""DBnomics extract — ดึง series เดียวคืน DataFrame (obs_date, value). ไม่แตะ DB, ไม่ต้อง key.
ใช้ backfill ประวัติย้อนหลัง (เช่น ดอกเบี้ยนโยบายไทยจาก BIS) ที่ BOT API ไม่มีให้."""
import pandas as pd
from dbnomics import fetch_series


def fetch(series_code: str) -> pd.DataFrame:
    df = fetch_series(series_code)[["period", "value"]].dropna()
    df = df.rename(columns={"period": "obs_date"})
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    return df.reset_index(drop=True)

    #BIS/WS_CBPOL/M.TH
