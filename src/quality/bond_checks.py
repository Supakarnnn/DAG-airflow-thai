"""DQ checks สำหรับ yield พันธบัตรรัฐบาลไทย 10Y (silver) ด้วย Pandera. รับ df คอลัมน์ bond_10y.
ไม่แตะ DB. ไม่เช็ค freshness — BOT Statistics publish ช้า ~1-2 เดือน (จะ false-positive)."""
import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

# BOT Statistics เสิร์ฟ history แค่ 2024-07+ (~23 เดือน) → ตั้ง MIN_ROWS ต่ำ
MIN_ROWS = 12

# data contract: yield 10Y ไทยยุคใหม่แกว่ง ~1-5%. กรอบ 0-15 หลวมเผื่อดอกเบี้ยสูงผิดปกติ
# (ยัง จับ corruption เช่นค่าติดลบ/หลักร้อยจาก basis-point ปน %)
BOND_SCHEMA = DataFrameSchema({
    "bond_10y": Column(float, Check.in_range(0, 15), nullable=False),
})


def validate_bond(df: pd.DataFrame) -> None:
    BOND_SCHEMA.validate(df, lazy=True)
    assert len(df) >= MIN_ROWS, f"แถวน้อยผิดปกติ: {len(df)} < {MIN_ROWS}"


if __name__ == "__main__":  # self-check: uv run -m src.quality.bond_checks  (offline)
    good = pd.DataFrame({"bond_10y": [2.5] * 24})
    validate_bond(good)
    print("OK: ข้อมูลดีผ่าน")

    bad = good.copy()
    bad.loc[0, "bond_10y"] = 264      # basis point หลุดมาปน % ต้องโดนจับ
    try:
        validate_bond(bad)
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except Exception as e:
        print(f"OK: จับค่าผิดได้ ({type(e).__name__})")
