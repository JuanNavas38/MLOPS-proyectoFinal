# MLOps Proyecto 2 — Diabetes Readmission Pipeline

**Pontificia Universidad Javeriana — Maestría en Computación de Alto Rendimiento**  
**Estudiantes:** Jhonthan Murcia Galán  
**Curso:** Operaciones de Machine Learning

---

## Descripción

Sistema MLOps completo desplegado en Kubernetes que implementa el ciclo de vida de un modelo de Machine Learning para predecir readmisión hospitalaria temprana (<30 días) en pacientes diabéticos. El dataset utilizado corresponde a 10 años (1999-2008) de atención clínica en 130 hospitales de EE.UU. con más de 100.000 registros.

---

## Arquitectura

```
Archivo CSV → Airflow DAG → PostgreSQL (raw/clean) → MLflow → API FastAPI → Streamlit
                                                          ↓
                                                       MinIO
                                                          ↓
                                              Prometheus → Grafana
                                                          ↑
                                                        Locust
```

### Componentes

| Componente | Tecnología | Namespace |
|---|---|---|
| Orquestación | Apache Airflow 3.2.0 (Helm) | airflow |
| Base de datos | PostgreSQL 15 | mlops |
| Object storage | MinIO | mlops |
| ML Tracking | MLflow 2.22.0 | mlops |
| API de inferencia | FastAPI + Uvicorn | mlops |
| Interfaz gráfica | Streamlit | mlops |
| Pruebas de carga | Locust 2.24.0 | mlops |
| Métricas | Prometheus + Grafana | mlops |

---

## Requisitos previos

- Rocky Linux 9.x
- k3s v1.34+ instalado
- Helm v4+
- Docker 29+
- kubectl configurado

---

## Despliegue

### 1. Clonar el repositorio

```bash
git clone https://github.com/masterofelectronic/mlops-proyecto2.git
cd mlops-proyecto2
```

### 2. Crear namespaces

```bash
kubectl apply -f k8s/namespace.yaml
kubectl create namespace airflow
```

### 3. Aplicar secrets

```bash
kubectl apply -f k8s/secrets.yaml
```

### 4. Desplegar PostgreSQL

```bash
kubectl apply -f k8s/postgres/configmap.yaml
kubectl apply -f k8s/postgres/pvc.yaml
kubectl apply -f k8s/postgres/statefulset.yaml
kubectl apply -f k8s/postgres/service.yaml
kubectl rollout status statefulset/postgres -n mlops --timeout=120s
```

### 5. Desplegar MinIO

```bash
kubectl apply -f k8s/minio/pvc.yaml
kubectl apply -f k8s/minio/deployment.yaml
kubectl apply -f k8s/minio/service.yaml
kubectl rollout status deployment/minio -n mlops --timeout=120s
kubectl apply -f k8s/minio/job-create-bucket.yaml
```

### 6. Desplegar MLflow

```bash
kubectl apply -f k8s/mlflow/deployment.yaml
kubectl apply -f k8s/mlflow/service.yaml
kubectl rollout status deployment/mlflow -n mlops --timeout=180s
```

### 7. Desplegar Airflow con Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values k8s/airflow/helm-values.yaml \
  --timeout 15m

# Exponer UI via NodePort
kubectl patch svc airflow-api-server -n airflow \
  -p '{"spec": {"type": "NodePort", "ports": [{"port": 8080, "targetPort": 8080, "nodePort": 30088}]}}'

# Crear usuario admin
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow users create \
  --username admin \
  --password admin2026 \
  --firstname Admin \
  --lastname MLOps \
  --role Admin \
  --email admin@mlops.com
```

### 8. Copiar DAG y dataset al pod de Airflow

```bash
SCHEDULER=$(kubectl get pod -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')

# Dataset
kubectl cp data/Diabetes.csv -n airflow $SCHEDULER:/opt/airflow/dags/Diabetes.csv -c scheduler

# DAG
kubectl cp dags/diabetes_pipeline.py -n airflow $SCHEDULER:/opt/airflow/dags/diabetes_pipeline.py -c scheduler
```

### 9. Crear Airflow Connection para PostgreSQL

```bash
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow connections add mlops_postgres \
  --conn-type postgres \
  --conn-host postgres-svc.mlops.svc.cluster.local \
  --conn-port 5432 \
  --conn-login mlops \
  --conn-password mlops2026 \
  --conn-schema mlops
```

### 10. Desplegar API de inferencia

```bash
kubectl apply -f k8s/api/deployment.yaml
kubectl apply -f k8s/api/service.yaml
kubectl rollout status deployment/diabetes-api -n mlops --timeout=120s
```

### 11. Desplegar Streamlit

```bash
kubectl apply -f k8s/streamlit/deployment.yaml
kubectl apply -f k8s/streamlit/service.yaml
kubectl rollout status deployment/diabetes-ui -n mlops --timeout=120s
```

### 12. Desplegar observabilidad

```bash
kubectl apply -f k8s/observability/prometheus/rbac.yaml
kubectl apply -f k8s/observability/prometheus/configmap.yaml
kubectl apply -f k8s/observability/prometheus/deployment.yaml
kubectl apply -f k8s/observability/grafana/deployment.yaml

kubectl rollout status deployment/prometheus -n mlops --timeout=120s
kubectl rollout status deployment/grafana -n mlops --timeout=120s
```

### 13. Desplegar Locust

```bash
kubectl create configmap locust-config \
  --from-file=locustfile.py=locust/locustfile.py -n mlops

kubectl apply -f k8s/locust/deployment.yaml
kubectl rollout status deployment/locust -n mlops --timeout=120s
```

---

## Acceso a los servicios

> Accesibles desde la red universitaria en `http://10.43.101.82:<puerto>`  
> Fuera de la red: configurar SSH tunnel via VPN universitaria

| Servicio | NodePort | URL local |
|---|---|---|
| Airflow UI | 30088 | http://localhost:30088 |
| MLflow UI | 30500 | http://localhost:30500 |
| MinIO Console | 30900 | http://localhost:30900 |
| FastAPI | 30800 | http://localhost:30800 |
| Streamlit | 30801 | http://localhost:30801 |
| Prometheus | 30909 | http://localhost:30909 |
| Grafana | 30300 | http://localhost:30300 |
| Locust | 30089 | http://localhost:30089 |

---

## Credenciales

| Servicio | Usuario | Contraseña |
|---|---|---|
| Airflow | admin | admin2026 |
| MLflow | — | sin autenticación |
| MinIO | minioadmin | minioadmin2026 |
| Grafana | admin | admin2026 |
| PostgreSQL | mlops | mlops2026 |

---

## Pipeline de datos

El DAG `diabetes_pipeline` implementa carga incremental en lotes de máximo 15.000 registros:

| Tarea | Descripción |
|---|---|
| `validate_source` | Verifica existencia y estructura del archivo CSV |
| `load_batch_to_raw` | Carga el siguiente lote a `raw_diabetes` con row_hash para deduplicación |
| `validate_data_quality` | Valida calidad del lote: nulos, rangos, valores válidos |
| `process_and_clean` | Limpieza, feature engineering y split 72/13/15 |
| `train_and_register` | Entrena LR + RF + XGBoost, registra en MLflow, promueve champion |

### Dataset — 101.766 registros — 7 lotes

| Lote | Registros | Estado |
|---|---|---|
| 1-6 | 15.000 c/u | ✅ Cargados |
| 7 | 6.766 | ✅ Cargado |

### Métrica de selección de modelo

Se usa **ROC-AUC** como métrica principal porque el dataset está desbalanceado (~11% readmisión temprana). En un contexto clínico, la capacidad discriminativa del modelo es más relevante que el accuracy global. Un falso negativo (no detectar readmisión) tiene mayor costo clínico que un falso positivo.

### Resultados primer experimento (lote 1 — 15.000 registros)

| Modelo | ROC-AUC | F1 |
|---|---|---|
| XGBoost | mejor | 0.796 |
| Random Forest | — | 0.839 |
| Logistic Regression | — | 0.708 |

**Modelo productivo:** `diabetes-champion` (XGBoost) con alias `champion` en MLflow.

---

## API de inferencia

### Endpoints

| Endpoint | Método | Descripción |
|---|---|---|
| `/health` | GET | Estado de la API y modelo cargado |
| `/predict` | POST | Predicción de readmisión |
| `/model-info` | GET | Nombre, versión y alias del modelo activo |
| `/metrics` | GET | Métricas Prometheus |

### Ejemplo de predicción

```bash
curl -X POST http://localhost:30800/predict \
  -H "Content-Type: application/json" \
  -d '{
    "time_in_hospital": 5,
    "num_lab_procedures": 45,
    "num_procedures": 2,
    "num_medications": 15,
    "number_outpatient": 0,
    "number_emergency": 1,
    "number_inpatient": 2,
    "number_diagnoses": 7,
    "age_encoded": 6,
    "admission_type_encoded": 1,
    "discharge_encoded": 1,
    "admission_source_encoded": 1,
    "insulin_encoded": 2,
    "change_encoded": 1,
    "diabetesmed_encoded": 1,
    "a1cresult_encoded": 0,
    "max_glu_serum_encoded": 0,
    "num_medications_log": 2.77,
    "service_utilization": 3
  }'
```

---

## Observabilidad

### Dashboards Grafana

| Dashboard | Descripción |
|---|---|
| MLOps API Dashboard | RPS, latencia, percentiles p50/p95/p99, errores, predicciones |
| MLOps Ingestion Dashboard | Registros raw/clean, lotes, distribución target, inferencias |

### Pruebas de carga — Locust

| Usuarios | RPS | Latencia promedio | p95 | Errores |
|---|---|---|---|---|
| 5 | ~4 req/s | ~25ms | ~38ms | 0% |
| 10 | ~8 req/s | ~24ms | ~38ms | 0% |

La API mantiene latencias estables al duplicar la carga. El límite operativo no fue alcanzado bajo las condiciones de recursos configuradas (1 CPU / 1Gi RAM).

---

## Estructura del repositorio

```
mlops-proyecto2/
├── k8s/
│   ├── namespace.yaml
│   ├── secrets.yaml
│   ├── postgres/
│   ├── minio/
│   ├── mlflow/
│   ├── airflow/
│   │   └── helm-values.yaml
│   ├── api/
│   ├── streamlit/
│   ├── locust/
│   └── observability/
│       ├── prometheus/
│       └── grafana/
├── dags/
│   └── diabetes_pipeline.py
├── src/
│   ├── api/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── ui/
│       ├── app.py
│       └── requirements.txt
├── docker/
│   ├── mlflow/Dockerfile
│   ├── api/Dockerfile
│   └── streamlit/Dockerfile
├── locust/
│   └── locustfile.py
├── migrations/
└── README.md
```

---

## Imágenes Docker

| Imagen | Tag | Descripción |
|---|---|---|
| `masterofelectronic/mlflow-postgres` | v2.22.0 | MLflow con psycopg2 + boto3 |
| `masterofelectronic/diabetes-api` | 1.0.1 | FastAPI de inferencia |
| `masterofelectronic/diabetes-ui` | 1.0.0 | Streamlit UI |

---

## Video de sustentación

[YouTube — MLOps Proyecto 2](https://youtu.be/PENDING)