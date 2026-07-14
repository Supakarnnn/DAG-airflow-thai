FROM apache/airflow:slim-2.9.3-python3.11

# Non-Spark DAG dependencies baked once; avoids pip work every container start.
RUN pip install --no-cache-dir \
    yfinance==1.4.1 fredapi==0.5.2 dbnomics==1.2.7 python-dotenv==1.1.1 \
    pandera==0.32.0 pandas==2.2.3 statsmodels==0.14.4 \
    apache-airflow-providers-postgres==5.11.2
