FROM apache/airflow:3.0.6-python3.12

# Versiones para el archivo de constraints oficial de Airflow.
# Instalar paquetes extra SIN constraints rompe deps de Airflow (sqlalchemy/flask-sqlalchemy).
ARG AIRFLOW_VERSION=3.0.6
ARG PYTHON_VERSION=3.12

USER airflow
# mlflow-skinny = cliente de tracking/registry SIN deps de servidor (Flask/sqlalchemy),
# que son las que chocan con las constraints de Airflow. Las tareas solo registran/consultan.
# Se relaja SOLO 'packaging' de las constraints (mlflow pide <25, Airflow funciona con 24).
ADD https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt /tmp/constraints.txt
USER root
RUN sed -i '/^packaging==/d' /tmp/constraints.txt && chmod 644 /tmp/constraints.txt
USER airflow
RUN pip install --no-cache-dir \
    "mlflow-skinny==2.21.3" \
    "scikit-learn==1.7.1" \
    xgboost \
    scipy \
    matplotlib \
    requests \
    psycopg2-binary \
    apache-airflow-providers-postgres \
    --constraint /tmp/constraints.txt

# Hornear DAGs y código del pipeline en la imagen (self-contained para GitOps).
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root src/  ${AIRFLOW_HOME}/src/
ENV PYTHONPATH=${AIRFLOW_HOME}/src
ENV PIPELINE_SRC=${AIRFLOW_HOME}/src
