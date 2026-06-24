"""SET pipeline: yfinance ^SET.BK -> bronze -> silver (2 indicators) -> gold (monthly + MoM)."""
import json
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from sqlalchemy import text

sys.path.append("/opt/airflow")
import pandas as pd  # noqa: E402
from src.extract.set import fetch_set  # noqa: E402
from src.quality.set_checks import validate_set  # noqa: E402

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()

# SET 1 ก้อนข้อมูล แตกเป็น 2 ตัวชี้วัดใน silver
INDICATORS = {
    "close":  dict(code="TH.SET_INDEX",  name_th="ดัชนีตลาดหลักทรัพย์ไทย", name_en="SET Index",  unit="points", category="market"),
    "volume": dict(code="TH.SET_VOLUME", name_th="ปริมาณซื้อขาย SET",      name_en="SET Volume", unit="shares", category="market"),
}


@dag(schedule="0 7 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["set", "phase0"])
def ingest_set():

    @task
    def to_bronze() -> None:
        df = fetch_set()
        payload = json.dumps({"obs": df.assign(obs_date=df.obs_date.astype(str)).to_dict("records")})
        run_id = get_current_context()["run_id"]
        with ENGINE().begin() as c:
            c.execute(text("""
                INSERT INTO bronze.raw_observation (batch_id, source_api, series_id, payload, request_params)
                VALUES (:b, 'SET', '^SET.BK', :p, :rp)
            """), {"b": run_id, "p": payload, "rp": json.dumps({"period": "5y"})})

    @task
    def to_silver() -> None:
        eng = ENGINE()
        with eng.begin() as c:
            row = c.execute(text("""
                SELECT payload FROM bronze.raw_observation
                WHERE series_id = '^SET.BK' ORDER BY ingested_at DESC LIMIT 1
            """)).scalar_one()
        obs = row["obs"]

        with eng.begin() as c:
            for field, meta in INDICATORS.items():
                c.execute(text("""
                    INSERT INTO silver.indicator_dim
                      (indicator_code,name_th,name_en,category,unit,frequency,source)
                    VALUES (:code,:th,:en,:cat,:unit,'D','SET')
                    ON CONFLICT (indicator_code) DO UPDATE SET name_en=EXCLUDED.name_en
                """), {"code": meta["code"], "th": meta["name_th"], "en": meta["name_en"],
                       "cat": meta["category"], "unit": meta["unit"]})
                for r in obs:
                    c.execute(text("""
                        INSERT INTO silver.macro_observation
                          (indicator_code,obs_date,value,unit,frequency,source,source_series_id)
                        VALUES (:code,:d,:v,:unit,'D','SET','^SET.BK')
                        ON CONFLICT (indicator_code,obs_date) DO UPDATE SET value=EXCLUDED.value
                    """), {"code": meta["code"], "d": r["obs_date"],
                           "v": float(r[field]), "unit": meta["unit"]})

    @task
    def validate_silver() -> None:
        # อ่าน SET จาก silver -> pivot wide -> Pandera ตรวจ. ไม่ผ่าน = raise -> to_gold ไม่รัน
        with ENGINE().begin() as c:
            rows = c.execute(text("""
                SELECT obs_date, indicator_code, value FROM silver.macro_observation
                WHERE indicator_code IN ('TH.SET_INDEX','TH.SET_VOLUME')
            """)).fetchall()
        df = pd.DataFrame(rows, columns=["obs_date", "indicator_code", "value"])
        wide = (df.pivot(index="obs_date", columns="indicator_code", values="value")
                  .rename(columns={"TH.SET_INDEX": "set_index", "TH.SET_VOLUME": "set_volume"})
                  .astype(float).reset_index())
        validate_set(wide)

    @task
    def to_gold() -> None:
        # set_close = close วันสุดท้ายของเดือน, set_volume = รวมทั้งเดือน, set_mom = %เทียบเดือนก่อน
        with ENGINE().begin() as c:
            c.execute(text("TRUNCATE gold.set_monthly"))
            c.execute(text("""
                INSERT INTO gold.set_monthly (obs_month, set_close, set_volume, set_mom)
                WITH close_m AS (
                  SELECT DISTINCT ON (date_trunc('month',obs_date))
                         date_trunc('month',obs_date)::date AS mth, value AS close
                  FROM silver.macro_observation WHERE indicator_code='TH.SET_INDEX'
                  ORDER BY date_trunc('month',obs_date), obs_date DESC
                ),
                vol_m AS (
                  SELECT date_trunc('month',obs_date)::date AS mth, sum(value) AS vol
                  FROM silver.macro_observation WHERE indicator_code='TH.SET_VOLUME' GROUP BY 1
                )
                SELECT c.mth, c.close, v.vol,
                       100*(c.close/lag(c.close) OVER (ORDER BY c.mth) - 1)
                FROM close_m c LEFT JOIN vol_m v USING (mth)
                ORDER BY c.mth
            """))

    to_bronze() >> to_silver() >> validate_silver() >> to_gold()


ingest_set()
