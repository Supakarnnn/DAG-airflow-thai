"""DQ checks สำหรับผลพยากรณ์ (gold.forecast_monthly แถว is_forecast=true) ด้วย Pandera.
รับ df คอลัมน์ yhat/yhat_lower/yhat_upper + จำนวนแถวที่คาดหวัง. ไม่แตะ DB.
ไม่เช็ค range สัมบูรณ์ (แต่ละ indicator สเกลต่างกัน) — เช็คความสมเหตุสมผลเชิงโครงสร้างพอ."""
import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

FC_SCHEMA = DataFrameSchema({
    "yhat":       Column(float, Check.gt(0), nullable=False),          # ค่าพยากรณ์ต้องเป็นบวก
    "yhat_lower": Column(float, nullable=False),
    "yhat_upper": Column(float, nullable=False),
}, checks=[
    # CI ต้องครอบ yhat: lower <= yhat <= upper ทุกแถว
    Check(lambda d: (d["yhat_lower"] <= d["yhat"]).all(), error="yhat_lower > yhat"),
    Check(lambda d: (d["yhat"] <= d["yhat_upper"]).all(), error="yhat > yhat_upper"),
])


def validate_forecast(df: pd.DataFrame, expected_periods: int) -> None:
    FC_SCHEMA.validate(df, lazy=True)
    assert len(df) == expected_periods, f"จำนวนเดือนพยากรณ์ผิด: {len(df)} != {expected_periods}"


if __name__ == "__main__":  # self-check: uv run -m src.quality.forecast_checks  (offline)
    good = pd.DataFrame({
        "yhat":       [1500.0, 1520.0, 1510.0],
        "yhat_lower": [1400.0, 1420.0, 1410.0],
        "yhat_upper": [1600.0, 1620.0, 1610.0],
    })
    validate_forecast(good, expected_periods=3)
    print("OK: ข้อมูลดีผ่าน")

    bad = good.copy()
    bad.loc[0, "yhat_upper"] = 1450.0     # upper < yhat ต้องโดนจับ
    try:
        validate_forecast(bad, expected_periods=3)
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except Exception as e:
        print(f"OK: จับ CI ผิดได้ ({type(e).__name__})")

    try:
        validate_forecast(good, expected_periods=6)   # นับแถวผิด ต้องโดนจับ
        raise SystemExit("FAIL: ควร raise แต่ไม่ raise")
    except AssertionError:
        print("OK: จับจำนวนแถวผิดได้")
