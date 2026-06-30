"""พยากรณ์ USD/THB รายเดือน 6 เดือนข้างหน้า (Holt-Winters) -> gold.forecast_monthly.
โครงเดียวกับ forecast_set (ใช้ forecast_series/validate_forecast ร่วม) ต่างแค่ source mart + CODE."""
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

sys.path.append("/opt/airflow")
import pandas as pd  # noqa: E402
from src.analytics.forecast import forecast_series  # noqa: E402
from src.quality.forecast_checks import validate_forecast  # noqa: E402

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()
CODE = "FX.USDTHB"
PERIODS = 6
MODEL = "holt-winters"


@dag(schedule="0 8 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["forecast", "phase2"])
def forecast_fx():

    @task
    def extract_actuals() -> list[dict]:
        with ENGINE().begin() as c:
            rows = c.execute(text("""
                SELECT obs_month, usdthb FROM gold.fx_monthly
                WHERE usdthb IS NOT NULL ORDER BY obs_month
            """)).fetchall()
        return [{"obs_month": str(r[0]), "value": float(r[1])} for r in rows]

    @task
    def make_forecast(actuals: list[dict]) -> list[dict]:
        fc = forecast_series(pd.DataFrame(actuals), periods=PERIODS)
        fc["obs_month"] = fc["obs_month"].astype(str)
        return fc.to_dict("records")

    @task
    def validate(fc: list[dict]) -> list[dict]:
        df = pd.DataFrame(fc).astype({"yhat": float, "yhat_lower": float, "yhat_upper": float})
        validate_forecast(df, expected_periods=PERIODS)   # gate: raise = ไม่เขียน gold
        return fc

    @task
    def to_gold(actuals: list[dict], fc: list[dict]) -> None:
        with ENGINE().begin() as c:
            c.execute(text("DELETE FROM gold.forecast_monthly WHERE indicator = :i"), {"i": CODE})
            for r in actuals:
                c.execute(text("""
                    INSERT INTO gold.forecast_monthly (obs_month, indicator, yhat, is_forecast)
                    VALUES (:d, :i, :y, false)
                """), {"d": r["obs_month"], "i": CODE, "y": r["value"]})
            for r in fc:
                c.execute(text("""
                    INSERT INTO gold.forecast_monthly
                      (obs_month, indicator, yhat, yhat_lower, yhat_upper, is_forecast, model)
                    VALUES (:d, :i, :y, :lo, :hi, true, :m)
                """), {"d": r["obs_month"], "i": CODE, "y": r["yhat"],
                       "lo": r["yhat_lower"], "hi": r["yhat_upper"], "m": MODEL})

    a = extract_actuals()
    to_gold(a, validate(make_forecast(a)))


forecast_fx()
