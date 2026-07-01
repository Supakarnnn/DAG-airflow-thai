"""พันธบัตรรัฐบาลไทย 10Y: BOT Statistics (FMRTINTM00296) -> bronze -> silver -> gold.bond_monthly.
BOT Statistics เสิร์ฟ history ~2024-07+ (สั้น) แต่ authoritative + fresh. obs_date เป็นวันที่ 1 ของเดือนอยู่แล้ว."""
import json
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from sqlalchemy import text

sys.path.append("/opt/airflow")
import pandas as pd  # noqa: E402
from src.extract.bot import fetch_bot_series  # noqa: E402
from src.quality.bond_checks import validate_bond  # noqa: E402

ENGINE = lambda: PostgresHook(postgres_conn_id="warehouse").get_sqlalchemy_engine()
CODE = "TH.BOND_10Y"
SERIES = "FMRTINTM00296"   # T-Bill & Government Bond Yield : 10 years (รายเดือน)


@dag(schedule="0 7 * * *", start_date=datetime(2024, 1, 1), catchup=False, tags=["bot", "phase2"])
def ingest_bond():

    @task
    def to_bronze() -> None:
        run_id = get_current_context()["run_id"]
        df = fetch_bot_series(SERIES)
        payload = json.dumps({"obs": df.assign(obs_date=df.obs_date.astype(str)).to_dict("records")})
        with ENGINE().begin() as c:
            c.execute(text("""
                INSERT INTO bronze.raw_observation (batch_id, source_api, series_id, payload, request_params)
                VALUES (:b, 'BOT_STAT', :sid, :p, :rp)
            """), {"b": run_id, "sid": SERIES, "p": payload, "rp": json.dumps({"code": CODE})})

    @task
    def to_silver() -> None:
        eng = ENGINE()
        with eng.begin() as c:
            row = c.execute(text("""
                SELECT payload FROM bronze.raw_observation
                WHERE series_id = :sid ORDER BY ingested_at DESC LIMIT 1
            """), {"sid": SERIES}).scalar_one()
            c.execute(text("""
                INSERT INTO silver.indicator_dim
                  (indicator_code,name_th,name_en,category,unit,frequency,source)
                VALUES (:code,'ผลตอบแทนพันธบัตรรัฐบาล 10 ปี','Govt Bond Yield 10Y','rates','percent','M','BOT')
                ON CONFLICT (indicator_code) DO UPDATE SET name_en=EXCLUDED.name_en
            """), {"code": CODE})
            for r in row["obs"]:
                c.execute(text("""
                    INSERT INTO silver.macro_observation
                      (indicator_code,obs_date,value,unit,frequency,source,source_series_id)
                    VALUES (:code,:d,:v,'percent','M','BOT',:sid)
                    ON CONFLICT (indicator_code,obs_date) DO UPDATE SET value=EXCLUDED.value
                """), {"code": CODE, "d": r["obs_date"], "v": float(r["value"]), "sid": SERIES})

    @task
    def validate_silver() -> None:
        with ENGINE().begin() as c:
            rows = c.execute(text("""
                SELECT value AS bond_10y FROM silver.macro_observation WHERE indicator_code = :code
            """), {"code": CODE}).fetchall()
        df = pd.DataFrame(rows, columns=["bond_10y"]).astype({"bond_10y": float})
        validate_bond(df)

    @task
    def to_gold() -> None:
        # obs_date เป็นวันที่ 1 ของเดือนอยู่แล้ว (BOT period yyyy-mm) → date_trunc = ค่าเดิม
        with ENGINE().begin() as c:
            c.execute(text("TRUNCATE gold.bond_monthly"))
            c.execute(text("""
                INSERT INTO gold.bond_monthly (obs_month, bond_10y)
                SELECT date_trunc('month', obs_date)::date, avg(value)
                FROM silver.macro_observation WHERE indicator_code = :code
                GROUP BY 1 ORDER BY 1
            """), {"code": CODE})

    to_bronze() >> to_silver() >> validate_silver() >> to_gold()


ingest_bond()
