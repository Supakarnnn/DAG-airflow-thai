# Airflow + Java + PySpark — สำหรับ DAG สาธิต Spark (ingest_set_spark)
# deps python อื่นยังลงผ่าน _PIP_ADDITIONAL_REQUIREMENTS เหมือนเดิม (ย้ายเข้ามาเมื่อ build เสถียร)
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.6 \
    # JDBC driver วางใน jars ของ pyspark เลย — ไม่ต้องพึ่ง Maven ตอนรัน
    && curl -fsSL -o /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/postgresql-42.7.4.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
