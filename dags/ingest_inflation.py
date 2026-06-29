"""เงินเฟ้อไทย: DBnomics IMF/IFS (CPI %YoY รายเดือน) -> silver -> gold.cpi_monthly.
real_rate = policy_rate - cpi_yoy เป็น VIEW (gold.real_rate_monthly) คำนวณสดจาก 2 mart."""
import json
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from sqlalchemy import text

sys.path.append("/opt/airflow")
import pandas as pd  # noqa: E402
from src.extract.dbnomics import fetch as fetch_db  # noqa: E402
from src.quality.inflation_checks import validate_cpi  # noqa: E402

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()
CODE = "TH.CPI_YOY"
CPI_SERIES = "IMF/IFS/M.TH.PCPI_PC_CP_A_PT"   # Thai headline CPI, % YoY, รายเดือน


@dag(schedule="0 7 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["inflation", "phase2"])
def ingest_inflation():

    @task
    def to_bronze() -> None:
        import logging
        run_id = get_current_context()["run_id"]
        # DBnomics soft-fail: ล่ม/timeout ก็ข้าม (ประวัติเดิมใน silver ยังอยู่ idempotent)
        try:
            df = fetch_db(CPI_SERIES)
        except Exception as e:
            logging.warning("ข้าม DBnomics CPI (best-effort): %s", e)
            return
        payload = json.dumps({"obs": df.assign(obs_date=df.obs_date.astype(str)).to_dict("records")})
        with ENGINE().begin() as c:
            c.execute(text("""
                INSERT INTO bronze.raw_observation (batch_id, source_api, series_id, payload, request_params)
                VALUES (:b, 'IMF', :sid, :p, :rp)
            """), {"b": run_id, "sid": CPI_SERIES, "p": payload, "rp": json.dumps({"code": CODE})})

    @task
    def to_silver() -> None:
        eng = ENGINE()
        with eng.begin() as c:
            row = c.execute(text("""
                SELECT payload FROM bronze.raw_observation
                WHERE series_id = :sid ORDER BY ingested_at DESC LIMIT 1
            """), {"sid": CPI_SERIES}).scalar_one_or_none()
            if row is None:        # ยังไม่เคยมี bronze (DBnomics ไม่เคยสำเร็จ) → ข้าม
                return
            c.execute(text("""
                INSERT INTO silver.indicator_dim
                  (indicator_code,name_th,name_en,category,unit,frequency,source)
                VALUES (:code,'เงินเฟ้อทั่วไป (YoY)','Headline CPI YoY','real','percent','M','IMF')
                ON CONFLICT (indicator_code) DO UPDATE SET name_en=EXCLUDED.name_en
            """), {"code": CODE})
            for r in row["obs"]:
                c.execute(text("""
                    INSERT INTO silver.macro_observation
                      (indicator_code,obs_date,value,unit,frequency,source,source_series_id)
                    VALUES (:code,:d,:v,'percent','M','IMF',:sid)
                    ON CONFLICT (indicator_code,obs_date) DO UPDATE SET value=EXCLUDED.value
                """), {"code": CODE, "d": r["obs_date"], "v": float(r["value"]), "sid": CPI_SERIES})

    @task
    def validate_silver() -> None:
        with ENGINE().begin() as c:
            rows = c.execute(text("""
                SELECT value AS cpi_yoy FROM silver.macro_observation WHERE indicator_code = :code
            """), {"code": CODE}).fetchall()
        df = pd.DataFrame(rows, columns=["cpi_yoy"]).astype({"cpi_yoy": float})
        validate_cpi(df)

    @task
    def to_gold() -> None:
        # silver (รายวัน/เดือน) -> รายเดือน (ค่าเดียวต่อเดือนอยู่แล้ว ใช้ avg กันซ้ำ)
        with ENGINE().begin() as c:
            c.execute(text("TRUNCATE gold.cpi_monthly"))
            c.execute(text("""
                INSERT INTO gold.cpi_monthly (obs_month, cpi_yoy)
                SELECT date_trunc('month', obs_date)::date AS mth, avg(value)
                FROM silver.macro_observation WHERE indicator_code = :code
                GROUP BY 1 ORDER BY 1
            """), {"code": CODE})

    to_bronze() >> to_silver() >> validate_silver() >> to_gold()


ingest_inflation()
