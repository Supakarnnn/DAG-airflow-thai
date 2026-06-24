"""DQ checks สำหรับ USD/THB (silver) ด้วย Pandera. รับ df ที่มีคอลัมน์ obs_date, usdthb.
ไม่แตะ DB — DAG อ่าน silver มาแล้วเรียก validate_fx(). ไม่ผ่าน = raise -> gold ไม่รัน."""
from datetime import date

import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

MAX_STALE_DAYS = 10   # FRED FX อัปเดตช้ากว่ารายวันจริงไม่กี่วันทำการ
MIN_ROWS = 100

# data contract: DEXTHUS ย้อนถึงปี 1981 — ครอบยุคบาทแข็ง ~20 + พีควิกฤต 1997 ~56
# กรอบ 18–60 กว้างพอรับประวัติศาสตร์จริง แต่ยังจับ corruption (0/ติดลบ/หลักร้อย)
FX_SCHEMA = DataFrameSchema({
    "usdthb": Column(float, Check.in_range(18, 60), nullable=False),
})


def validate_fx(df: pd.DataFrame, today: date | None = None) -> None:
    today = today or date.today()
    FX_SCHEMA.validate(df, lazy=True)

    assert len(df) >= MIN_ROWS, f"แถวน้อยผิดปกติ: {len(df)} < {MIN_ROWS}"

    stale = (today - pd.to_datetime(df["obs_date"]).max().date()).days
    assert stale <= MAX_STALE_DAYS, f"ข้อมูลเก่าไป {stale} วัน (เกิน {MAX_STALE_DAYS})"


if __name__ == "__main__":  # self-check: uv run -m src.quality.fred_checks
    good = pd.DataFrame({"obs_date": pd.date_range("2024-01-01", periods=200).date, "usdthb": 33.0})
    validate_fx(good, today=good["obs_date"].max())
    print("OK: ข้อมูลดีผ่าน")

    bad = good.copy(); bad.loc[0, "usdthb"] = 99      # ค่าเพี้ยนนอกกรอบ ต้องโดนจับ
    try:
        validate_fx(bad, today=bad["obs_date"].max())
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except Exception as e:
        print(f"OK: จับค่าผิดได้ ({type(e).__name__})")
