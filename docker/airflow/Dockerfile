FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.0 \
    psycopg2-binary==2.9.9 \
    mlflow==2.22.0 \
    scikit-learn==1.4.2 \
    xgboost==2.0.3 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    boto3==1.34.0 \
    sqlalchemy==1.4.52