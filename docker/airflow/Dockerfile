FROM apache/airflow:3.2.0

USER airflow
RUN pip install --no-cache-dir \
    mlflow==2.21.3 \
    scikit-learn==1.5.2 \
    xgboost==2.1.3 \
    pandas==2.2.3 \
    numpy==2.1.3 \
    scipy==1.14.1 \
    matplotlib==3.9.2 \
    requests==2.32.3 \
    psycopg2-binary==2.9.10 \
    apache-airflow-providers-postgres==6.6.1
