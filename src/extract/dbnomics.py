"""DBnomics extract — ดึง series เดียวคืน DataFrame (obs_date, value). ไม่แตะ DB, ไม่ต้อง key.
ใช้ backfill ประวัติย้อนหลัง (เช่น ดอกเบี้ยนโยบายไทยจาก BIS) ที่ BOT API ไม่มีให้."""
import pandas as pd
from dbnomics import fetch_series


def fetch(series_code: str) -> pd.DataFrame:
    """series_code เช่น 'BIS/WS_CBPOL/M.TH'. คืน obs_date (date), value."""
    df = fetch_series(series_code)[["period", "value"]].dropna()
    df = df.rename(columns={"period": "obs_date"})
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    return df.reset_index(drop=True)


if __name__ == "__main__":  # self-check: uv run -m src.extract.dbnomics
    out = fetch("BIS/WS_CBPOL/M.TH")
    assert list(out.columns) == ["obs_date", "value"], "คอลัมน์ผิด"
    assert out["value"].notna().all(), "มี NaN หลุดมา"
    print(f"OK: {len(out)} แถว, ล่าสุด {out.obs_date.iloc[-1]} = {out.value.iloc[-1]}%")
