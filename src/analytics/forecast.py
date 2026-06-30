"""Forecast time series รายเดือนด้วย Holt-Winters (statsmodels). pure — ไม่แตะ DB.
รับ df[obs_month, value] เรียงเก่า→ใหม่ คืน df[obs_month, yhat, yhat_lower, yhat_upper] ของอนาคต."""
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

SEASON = 12  # รายเดือน → วงรอบ 1 ปี


def forecast_series(df: pd.DataFrame, periods: int = 6, season: int = SEASON) -> pd.DataFrame:
    """Holt-Winters additive (trend+seasonal). df ต้องมี obs_month (date), value (float) เรียงขึ้น.
    ต้องมีอย่างน้อย 2*season จุด ไม่งั้น seasonal ประมาณไม่ได้."""
    s = df.sort_values("obs_month").reset_index(drop=True)
    assert len(s) >= 2 * season, f"ข้อมูลน้อยไป {len(s)} < {2 * season} จุด (ต้องครอบ 2 รอบฤดูกาล)"

    fit = ExponentialSmoothing(
        s["value"].astype(float), trend="add", seasonal="add", seasonal_periods=season
    ).fit()
    yhat = fit.forecast(periods)

    # ponytail: statsmodels HW ไม่มี prediction interval ในตัว → ใช้ ±1.96σ ของ residual
    # คงที่ (ไม่ขยายตามระยะพยากรณ์). อัปเกรดเป็น simulate() ถ้าต้อง interval ที่ถูกต้องกว่า
    sigma = (fit.fittedvalues - s["value"].astype(float)).std()
    band = 1.96 * sigma

    last = pd.Timestamp(s["obs_month"].iloc[-1])
    future = pd.date_range(last + pd.offsets.MonthBegin(1), periods=periods, freq="MS").date
    return pd.DataFrame({
        "obs_month": future,
        "yhat": yhat.to_numpy().round(2),
        "yhat_lower": (yhat.to_numpy() - band).round(2),
        "yhat_upper": (yhat.to_numpy() + band).round(2),
    })


if __name__ == "__main__":  # self-check: uv run -m src.analytics.forecast  (ต้อง postgres รันที่ :5433)
    from sqlalchemy import create_engine
    eng = create_engine("postgresql+psycopg2://warehouse:warehouse@127.0.0.1:5433/warehouse")
    actual = pd.read_sql("SELECT obs_month, set_close AS value FROM gold.set_monthly ORDER BY obs_month", eng)

    fc = forecast_series(actual, periods=6)
    print(f"actual: {len(actual)} เดือน, ล่าสุด {actual.obs_month.iloc[-1]} = {actual.value.iloc[-1]:.0f}")
    print(fc.to_string(index=False))

    last_val = float(actual.value.iloc[-1])
    assert (fc.yhat > 0).all(), "forecast ติดลบ ผิดปกติ"
    assert fc.yhat.between(last_val / 3, last_val * 3).all(), "forecast หลุดช่วงสมเหตุสมผล (>3x/<1/3)"
    assert (fc.yhat_lower <= fc.yhat).all() and (fc.yhat <= fc.yhat_upper).all(), "CI ครอบ yhat ไม่ถูก"
    print("OK: forecast SET ผ่าน self-check")
