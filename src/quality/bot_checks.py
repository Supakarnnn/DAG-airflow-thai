"""DQ checks สำหรับดอกเบี้ยนโยบายไทย (silver) ด้วย Pandera. รับ df ที่มีคอลัมน์ policy_rate.
ไม่แตะ DB — DAG อ่าน silver มาแล้วเรียก validate_policy_rate(). ไม่ผ่าน = raise -> gold ไม่รัน.

หมายเหตุ: ไม่เช็ค freshness — policy rate เป็น event-based เปลี่ยนแค่ตอน กนง.ประชุม
ค่าล่าสุดอาจเก่าเป็นเดือนได้อย่างถูกต้อง (ดอกเบี้ยคงที่ระหว่างประชุม) เช็ค stale = false-positive."""
import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

MIN_ROWS = 100

# data contract: ดอกเบี้ยนโยบายไทย (BIS series ตั้งแต่ ~2000) อยู่ราว 0.5–5%
# กรอบ 0–12 ครอบช่วงดอกเบี้ยสูงยุคก่อน + ยังจับ corruption (ติดลบ/หลักสิบหลายตัว)
RATE_SCHEMA = DataFrameSchema({
    "policy_rate": Column(float, Check.in_range(0, 12), nullable=False),
})


def validate_policy_rate(df: pd.DataFrame) -> None:
    RATE_SCHEMA.validate(df, lazy=True)
    assert len(df) >= MIN_ROWS, f"แถวน้อยผิดปกติ: {len(df)} < {MIN_ROWS}"


if __name__ == "__main__":  # self-check: uv run -m src.quality.bot_checks
    good = pd.DataFrame({"policy_rate": [1.75] * 200})
    validate_policy_rate(good)
    print("OK: ข้อมูลดีผ่าน")

    bad = good.copy()
    bad.loc[0, "policy_rate"] = -1     # ติดลบ ต้องโดนจับ
    try:
        validate_policy_rate(bad)
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except Exception as e:
        print(f"OK: จับค่าผิดได้ ({type(e).__name__})")
