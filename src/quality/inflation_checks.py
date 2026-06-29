"""DQ checks สำหรับเงินเฟ้อไทย CPI %YoY (silver) ด้วย Pandera. รับ df คอลัมน์ cpi_yoy.
ไม่แตะ DB. ไม่เช็ค freshness — DBnomics IMF lag ~15 เดือน (จะ false-positive)."""
import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

MIN_ROWS = 100

# data contract: IMF series ย้อนถึง 1966 — เงินเฟ้อไทยพีค ~28% ยุควิกฤตน้ำมัน 1973-74/1980,
# ต่ำสุด ~ -4% (เงินฝืด). กรอบ -8 ถึง 50 ตั้งหลวมเผื่อไว้ก่อน (ยังจับ corruption เช่นค่าหลักร้อย/ติดลบมาก)
CPI_SCHEMA = DataFrameSchema({
    "cpi_yoy": Column(float, Check.in_range(-8, 50), nullable=False),
})


def validate_cpi(df: pd.DataFrame) -> None:
    CPI_SCHEMA.validate(df, lazy=True)
    assert len(df) >= MIN_ROWS, f"แถวน้อยผิดปกติ: {len(df)} < {MIN_ROWS}"


if __name__ == "__main__":  # self-check: uv run -m src.quality.inflation_checks
    good = pd.DataFrame({"cpi_yoy": [1.2] * 200})
    validate_cpi(good)
    print("OK: ข้อมูลดีผ่าน")

    bad = good.copy()
    bad.loc[0, "cpi_yoy"] = 99      # นอกกรอบ ต้องโดนจับ
    try:
        validate_cpi(bad)
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except Exception as e:
        print(f"OK: จับค่าผิดได้ ({type(e).__name__})")
