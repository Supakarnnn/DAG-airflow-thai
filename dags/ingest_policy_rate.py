"""ดอกเบี้ยนโยบายไทย: BOT (ค่าสด) + DBnomics (backfill ย้อนหลัง) -> silver -> gold (forward-fill).
BOT API ให้แค่ค่าปัจจุบัน+วันมีผล, DBnomics เติมประวัติเก่า — รวมเป็น series เดียวใน silver."""
import json
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from sqlalchemy import text

sys.path.append("/opt/airflow")
from src.extract.bot import fetch_policy_rate       # noqa: E402
from src.extract.dbnomics import fetch as fetch_db  # noqa: E402

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()
CODE = "TH.POLICY_RATE"
BACKFILL_SERIES = "BIS/WS_CBPOL/M.TH"   # ประวัติย้อนหลังจาก BIS ผ่าน DBnomics


@dag(schedule="0 7 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["bot", "policy-rate"])
def ingest_policy_rate():

    @task
    def to_bronze() -> None:
        run_id = get_current_context()["run_id"]
        eng = ENGINE()
        # BOT: ค่าสดปัจจุบัน (เก็บ news ไว้ใน payload ด้วย ไว้ป้อน LLM ทีหลัง)
        bot = fetch_policy_rate()
        # DBnomics: ประวัติย้อนหลังทั้งชุด
        hist = fetch_db(BACKFILL_SERIES)
        hist_payload = {"obs": hist.assign(obs_date=hist.obs_date.astype(str)).to_dict("records")}
        with eng.begin() as c:
            for sid, payload in [("BOT/policy_rate", bot), (BACKFILL_SERIES, hist_payload)]:
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

            def upsert(obs_date, value):
                c.execute(text("""
                    INSERT INTO silver.macro_observation
                      (indicator_code,obs_date,value,unit,frequency,source,source_series_id)
                    VALUES (:code,:d,:v,'percent','M','BOT',:sid)
                    ON CONFLICT (indicator_code,obs_date) DO UPDATE SET value=EXCLUDED.value
                """), {"code": CODE, "d": obs_date, "v": float(value), "sid": "policy_rate"})

            # DBnomics history (เก่า) ก่อน แล้ว BOT (สด) ทับ — ถ้าวันชนกัน BOT ชนะ
            for sid in [BACKFILL_SERIES, "BOT/policy_rate"]:
                row = c.execute(text("""
                    SELECT payload FROM bronze.raw_observation
                    WHERE series_id = :sid ORDER BY ingested_at DESC LIMIT 1
                """), {"sid": sid}).scalar_one()
                if sid == "BOT/policy_rate":
                    upsert(row["obs_date"], row["value"])
                else:
                    for r in row["obs"]:
                        upsert(r["obs_date"], r["value"])

    @task
    def to_gold() -> None:
        # สร้างเดือนต่อเนื่องตั้งแต่ข้อมูลแรกถึงปัจจุบัน แล้ว as-of fill (ค่าที่มีผล ณ สิ้นเดือน)
        # ponytail: correlated subquery — ข้อมูลหลักร้อยเดือน เร็วพอ; เปลี่ยนเป็น window ถ้าโตมาก
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

    to_bronze() >> to_silver() >> to_gold()


ingest_policy_rate()
