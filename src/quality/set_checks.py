"""DQ checks สำหรับ SET (silver) ด้วย Pandera. รับ wide df (obs_date,set_index,set_volume).
ไม่แตะ DB — DAG อ่าน silver มาแล้วเรียก validate_set(). ไม่ผ่าน = raise -> DAG แดง -> gold ไม่รัน."""
from datetime import date

import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

MAX_STALE_DAYS = 7   # SET รายวัน เผื่อ เสาร์-อาทิตย์+วันหยุด
MIN_ROWS = 100

# data contract: ช่วงค่าสมเหตุสมผล + ห้าม null (SET ในประวัติศาสตร์ ~200–1800)
SET_SCHEMA = DataFrameSchema({
    "set_index":  Column(float, Check.in_range(50, 5000), nullable=False),
    "set_volume": Column(float, Check.ge(0), nullable=False),
})


def validate_set(df: pd.DataFrame, today: date | None = None) -> None:
    today = today or date.today()
    SET_SCHEMA.validate(df, lazy=True)                       # range + null (รวมทุก error)

    assert len(df) >= MIN_ROWS, f"แถวน้อยผิดปกติ: {len(df)} < {MIN_ROWS}"

    stale = (today - pd.to_datetime(df["obs_date"]).max().date()).days
    assert stale <= MAX_STALE_DAYS, f"ข้อมูลเก่าไป {stale} วัน (เกิน {MAX_STALE_DAYS})"


if __name__ == "__main__":  # self-check: uv run -m src.quality.set_checks
    good = pd.DataFrame({"obs_date": pd.date_range("2024-01-01", periods=200).date,
                         "set_index": 1500.0, "set_volume": 1e6})
    validate_set(good, today=good["obs_date"].max())
    print("OK: ข้อมูลดีผ่าน")

    bad = good.copy()
    bad.loc[0, "set_index"] = -1      # ค่าติดลบ ต้องโดนจับ
    try:
        validate_set(bad, today=bad["obs_date"].max())
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except Exception as e:
        print(f"OK: จับค่าผิดได้ ({type(e).__name__})")
