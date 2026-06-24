"""FRED pipeline: USD/THB -> bronze -> silver -> gold.fx_monthly (monthly + MoM).
เพิ่ม series ใหม่ (เช่น ดอกเบี้ยไทย) แค่เติมใน SERIES + เพิ่ม gold ของมัน."""
import json
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from sqlalchemy import text

sys.path.append("/opt/airflow")
from src.extract.fred import fetch_series  # noqa: E402

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()

SERIES = [
    dict(series_id="DEXTHUS", code="FX.USDTHB", name_th="อัตราแลกเปลี่ยน USD/THB",
         name_en="USD/THB Exchange Rate", unit="THB per USD", category="external", freq="D"),
]


@dag(schedule="0 7 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["fred", "phase0"])
def ingest_fred():

    @task
    def to_bronze() -> None:
        run_id = get_current_context()["run_id"]
        eng = ENGINE()
        for s in SERIES:
            df = fetch_series(s["series_id"])
            payload = json.dumps({"obs": df.assign(obs_date=df.obs_date.astype(str)).to_dict("records")})
            with eng.begin() as c:
                c.execute(text("""
                    INSERT INTO bronze.raw_observation (batch_id, source_api, series_id, payload, request_params)
                    VALUES (:b, 'FRED', :sid, :p, :rp)
                """), {"b": run_id, "sid": s["series_id"], "p": payload,
                       "rp": json.dumps({"code": s["code"]})})

    @task
    def to_silver() -> None:
        eng = ENGINE()
        for s in SERIES:
            with eng.begin() as c:
                row = c.execute(text("""
                    SELECT payload FROM bronze.raw_observation
                    WHERE series_id = :sid ORDER BY ingested_at DESC LIMIT 1
                """), {"sid": s["series_id"]}).scalar_one()
            with eng.begin() as c:
                c.execute(text("""
                    INSERT INTO silver.indicator_dim
                      (indicator_code,name_th,name_en,category,unit,frequency,source)
                    VALUES (:code,:th,:en,:cat,:unit,:freq,'FRED')
                    ON CONFLICT (indicator_code) DO UPDATE SET name_en=EXCLUDED.name_en
                """), {"code": s["code"], "th": s["name_th"], "en": s["name_en"],
                       "cat": s["category"], "unit": s["unit"], "freq": s["freq"]})
                for r in row["obs"]:
                    c.execute(text("""
                        INSERT INTO silver.macro_observation
                          (indicator_code,obs_date,value,unit,frequency,source,source_series_id)
                        VALUES (:code,:d,:v,:unit,:freq,'FRED',:sid)
                        ON CONFLICT (indicator_code,obs_date) DO UPDATE SET value=EXCLUDED.value
                    """), {"code": s["code"], "d": r["obs_date"], "v": float(r["value"]),
                           "unit": s["unit"], "freq": s["freq"], "sid": s["series_id"]})

    @task
    def to_gold() -> None:
        # usdthb สิ้นเดือน (DISTINCT ON) + MoM% (lag)
        with ENGINE().begin() as c:
            c.execute(text("TRUNCATE gold.fx_monthly"))
            c.execute(text("""
                INSERT INTO gold.fx_monthly (obs_month, usdthb, usdthb_mom)
                WITH m AS (
                  SELECT DISTINCT ON (date_trunc('month',obs_date))
                         date_trunc('month',obs_date)::date AS mth, value AS usdthb
                  FROM silver.macro_observation WHERE indicator_code='FX.USDTHB'
                  ORDER BY date_trunc('month',obs_date), obs_date DESC
                )
                SELECT mth, usdthb, 100*(usdthb/lag(usdthb) OVER (ORDER BY mth) - 1)
                FROM m ORDER BY mth
            """))

    to_bronze() >> to_silver() >> to_gold()


ingest_fred()
