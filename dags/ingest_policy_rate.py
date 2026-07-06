import json
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from sqlalchemy import text

sys.path.append("/opt/airflow")
import pandas as pd
from src.extract.bot import fetch_policy_rate, fetch_bot_series
from src.extract.dbnomics import fetch as fetch_db
from src.quality.bot_checks import validate_policy_rate

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()
CODE = "TH.POLICY_RATE"
BACKFILL_SERIES = "BIS/WS_CBPOL/M.TH"   # ประวัติย้อนหลังจาก BIS ผ่าน DBnomics (2000→~2025-06)
STAT_SERIES = "FMRTINTM00262"           # BOT Statistics: policy rate รายเดือน (2024-07→now) เติมช่อง forward-fill


@dag(schedule="0 7 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["bot", "policy-rate"])
def ingest_policy_rate():

    @task
    def to_bronze() -> None:
        import logging
        run_id = get_current_context()["run_id"]
        eng = ENGINE()
        rows = [("BOT/policy_rate", fetch_policy_rate())]
        try:
            hist = fetch_db(BACKFILL_SERIES)
            rows.append((BACKFILL_SERIES,
                         {"obs": hist.assign(obs_date=hist.obs_date.astype(str)).to_dict("records")}))
        except Exception as e:
            logging.warning("ข้าม DBnomics backfill (best-effort): %s", e)
        try:
            stat = fetch_bot_series(STAT_SERIES)
            rows.append((STAT_SERIES,
                         {"obs": stat.assign(obs_date=stat.obs_date.astype(str)).to_dict("records")}))
        except Exception as e:
            logging.warning("ข้าม BOT Statistics %s (best-effort): %s", STAT_SERIES, e)
        with eng.begin() as c:
            for sid, payload in rows:
                c.execute(text("""
                    INSERT INTO bronze.raw_observation (batch_id, source_api, series_id, payload, request_params)
                    VALUES (:b, 'BOT', :sid, :p, :rp)
                """), {"b": run_id, "sid": sid, "p": json.dumps(payload),
                       "rp": json.dumps({"code": CODE})})

    @task
    def to_silver() -> None:
        eng = ENGINE()
        with eng.begin() as c:
            c.execute(text("""
                INSERT INTO silver.indicator_dim
                  (indicator_code,name_th,name_en,category,unit,frequency,source)
                VALUES (:code,'อัตราดอกเบี้ยนโยบาย','Policy Rate','monetary','percent','M','BOT')
                ON CONFLICT (indicator_code) DO UPDATE SET name_en=EXCLUDED.name_en
            """), {"code": CODE})

            def upsert(obs_date, value, source, series_id):
                c.execute(text("""
                    INSERT INTO silver.macro_observation
                      (indicator_code,obs_date,value,unit,frequency,source,source_series_id)
                    VALUES (:code,:d,:v,'percent','M',:src,:sid)
                    ON CONFLICT (indicator_code,obs_date)
                    DO UPDATE SET value=EXCLUDED.value, source=EXCLUDED.source, source_series_id=EXCLUDED.source_series_id
                """), {"code": CODE, "d": obs_date, "v": float(value), "src": source, "sid": series_id})

            layers = [
                (BACKFILL_SERIES,   "BIS",      BACKFILL_SERIES),
                (STAT_SERIES,       "BOT_STAT", STAT_SERIES),
                ("BOT/policy_rate", "BOT",      "policy_rate"),
            ]
            for bronze_sid, src, series_id in layers:
                row = c.execute(text("""
                    SELECT payload FROM bronze.raw_observation
                    WHERE series_id = :sid ORDER BY ingested_at DESC LIMIT 1
                """), {"sid": bronze_sid}).scalar_one_or_none()
                if row is None:
                    continue
                if bronze_sid == "BOT/policy_rate":
                    upsert(row["obs_date"], row["value"], src, series_id)
                else:
                    for r in row["obs"]:
                        upsert(r["obs_date"], r["value"], src, series_id)

    @task
    def validate_silver() -> None:
        with ENGINE().begin() as c:
            rows = c.execute(text("""
                SELECT value AS policy_rate FROM silver.macro_observation
                WHERE indicator_code = :code
            """), {"code": CODE}).fetchall()
        df = pd.DataFrame(rows, columns=["policy_rate"]).astype({"policy_rate": float})
        validate_policy_rate(df)

    @task
    def to_gold() -> None:
        with ENGINE().begin() as c:
            c.execute(text("TRUNCATE gold.policy_rate_monthly"))
            c.execute(text("""
                INSERT INTO gold.policy_rate_monthly (obs_month, policy_rate)
                WITH bounds AS (
                  SELECT date_trunc('month', min(obs_date))::date AS lo
                  FROM silver.macro_observation WHERE indicator_code=:code
                ),
                months AS (
                  SELECT generate_series(lo, date_trunc('month', CURRENT_DATE)::date, interval '1 month')::date AS mth
                  FROM bounds
                )
                SELECT m.mth,
                       (SELECT s.value FROM silver.macro_observation s
                        WHERE s.indicator_code=:code
                          AND s.obs_date <= (m.mth + interval '1 month' - interval '1 day')::date
                        ORDER BY s.obs_date DESC LIMIT 1) AS policy_rate
                FROM months m
                WHERE (SELECT s.value FROM silver.macro_observation s
                       WHERE s.indicator_code=:code
                         AND s.obs_date <= (m.mth + interval '1 month' - interval '1 day')::date
                       LIMIT 1) IS NOT NULL
                ORDER BY m.mth
            """), {"code": CODE})

    to_bronze() >> to_silver() >> validate_silver() >> to_gold()

ingest_policy_rate()
