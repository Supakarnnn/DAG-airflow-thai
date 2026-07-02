"""just only demo spark"""
import sys
from datetime import date, datetime

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")

LAKE = "/opt/airflow/data/spark"
JDBC_URL = "jdbc:postgresql://postgres:5432/warehouse"
JDBC_PROPS = {"user": "warehouse", "password": "warehouse", "driver": "org.postgresql.Driver"}


def spark_session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName("ingest_set_spark")
            .master("local[*]")                      # ทุก core ในคอนเทนเนอร์ ไม่มี cluster
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["set", "spark", "demo"])
def ingest_set_spark():

    @task
    def bronze_spark() -> None:
        # extract ยัง reuse client เดิม (yfinance คืน pandas) → แปลงเป็น Spark df → เก็บดิบเป็น parquet
        from src.extract.set import fetch_set
        pdf = fetch_set()
        spark = spark_session()
        try:
            spark.createDataFrame(pdf).write.mode("overwrite").parquet(f"{LAKE}/bronze/set")
        finally:
            spark.stop()

    @task
    def silver_spark() -> None:
        # wide -> long (2 indicators) ด้วย stack + dedupe = งานเดียวกับ upsert เข้า silver
        from pyspark.sql import functions as F
        spark = spark_session()
        try:
            (spark.read.parquet(f"{LAKE}/bronze/set")
             .select("obs_date",
                     F.expr("stack(2, 'TH.SET_INDEX', close, 'TH.SET_VOLUME', double(volume)) "
                            "as (indicator_code, value)"))
             .dropDuplicates(["indicator_code", "obs_date"])
             .write.mode("overwrite").partitionBy("indicator_code")
             .parquet(f"{LAKE}/silver/set"))
        finally:
            spark.stop()

    @task
    def dq_spark() -> None:
        # กติกาเดียวกับ src/quality/set_checks.py (Pandera ใช้กับ Spark df ตรง ๆ ไม่ได้)
        from pyspark.sql import functions as F
        spark = spark_session()
        try:
            df = spark.read.parquet(f"{LAKE}/silver/set")
            n_dates = df.select("obs_date").distinct().count()
            assert n_dates >= 100, f"แถวน้อยผิดปกติ: {n_dates} < 100"
            assert df.filter(F.col("value").isNull()).count() == 0, "มีค่า null"
            bad = df.filter((F.col("indicator_code") == "TH.SET_INDEX")
                            & ~F.col("value").between(50, 5000)).count()
            assert bad == 0, f"SET index หลุดช่วง 50-5000: {bad} แถว"
            assert df.filter(F.col("value") < 0).count() == 0, "มีค่าติดลบ"
            latest = df.agg(F.max("obs_date")).first()[0]
            stale = (date.today() - latest).days
            assert stale <= 7, f"ข้อมูลเก่าไป {stale} วัน (เกิน 7)"
        finally:
            spark.stop()

    @task
    def gold_spark() -> None:
        # aggregate รายเดือนด้วย window function แล้วเขียน Postgres ผ่าน JDBC
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        spark = spark_session()
        try:
            df = (spark.read.parquet(f"{LAKE}/silver/set")
                  .withColumn("obs_month", F.trunc("obs_date", "month")))

            # close สิ้นเดือน = แถวล่าสุดของเดือน (เทียบ DISTINCT ON ... ORDER BY obs_date DESC)
            w_last = Window.partitionBy("obs_month").orderBy(F.col("obs_date").desc())
            close_m = (df.filter(F.col("indicator_code") == "TH.SET_INDEX")
                       .withColumn("rn", F.row_number().over(w_last))
                       .filter("rn = 1").select("obs_month", F.col("value").alias("set_close")))
            vol_m = (df.filter(F.col("indicator_code") == "TH.SET_VOLUME")
                     .groupBy("obs_month").agg(F.sum("value").alias("set_volume")))

            w_mom = Window.orderBy("obs_month")
            gold = (close_m.join(vol_m, "obs_month", "left")
                    .withColumn("set_mom",
                                100 * (F.col("set_close") / F.lag("set_close").over(w_mom) - 1))
                    .orderBy("obs_month"))

            # truncate=true = ล้างข้อมูลแต่คงตาราง/PK เดิม (เทียบ TRUNCATE+INSERT ของ DAG pandas)
            (gold.write.mode("overwrite").option("truncate", "true")
             .jdbc(JDBC_URL, "gold.set_monthly_spark", properties=JDBC_PROPS))
        finally:
            spark.stop()

    bronze_spark() >> silver_spark() >> dq_spark() >> gold_spark()


ingest_set_spark()
