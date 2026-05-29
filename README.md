# MLOps Proyecto Final — Nivel 4: Automatización, decisión de reentrenamiento y despliegue GitOps

**Pontificia Universidad Javeriana — Maestría en Inteligencia Artificial**
**Curso:** Operaciones de Machine Learning
**Estudiantes:** _(por completar)_

> Sistema MLOps que recolecta datos por lotes desde una API externa, los valida y procesa,
> **decide automáticamente si reentrenar**, registra experimentos en MLflow, **promueve el
> modelo solo si mejora al productivo**, y expone inferencia vía FastAPI tomando MLflow como
> única fuente de verdad. Todo desplegado en Kubernetes y sincronizado con **Argo CD (GitOps)**.

> ⚠️ **Estado: ESQUELETO.** La infraestructura (K8s, MLflow, Postgres, MinIO, observabilidad,
> Locust) se reutiliza de una entrega previa y ya está neutralizada al dominio inmobiliario.
> La lógica nueva del Nivel 4 (cliente API, bifurcaciones del DAG, comparación/promoción,
> recarga RF7, CI y Argo CD) está marcada con `TODO` y se implementa por fases.

---

## Problema

**Regresión:** estimar el `price` de una propiedad a partir de 12 variables estructurales,
geográficas y comerciales (bed, bath, acre_lot, house_size, city, state, zip_code, etc.).

Los datos **no se entregan completos**: llegan **por lotes** desde una API externa
(`cristiandiaz13/mlops-puj:data-api-pf-v1`). Cada ejecución del DAG consume un lote y decide,
con reglas técnicas, si amerita reentrenar.

**Métrica prioritaria:** MAE (con RMSE, MAPE y R² de apoyo).
**Regla de promoción:** promover el candidato solo si **MAE baja ≥ 3 %** y **RMSE no empeora > 1 %**.

---

## Arquitectura

```
Usuario → GitHub → GitHub Actions → DockerHub
                                        │
                                   Argo CD (GitOps)
                                        │
                                   Kubernetes
   ┌────────────────────────────────────────────────────────────┐
   │  API de datos (lotes) → Airflow DAG → PostgreSQL (RAW/CLEAN) │
   │                              │                               │
   │                           MLflow ── Postgres + MinIO         │
   │                              │                               │
   │   FastAPI (carga modelo desde MLflow, recarga sin redeploy)  │
   │        │                 │                                   │
   │   Streamlit          Prometheus → Grafana   ← Locust         │
   └────────────────────────────────────────────────────────────┘
```

### Componentes

| Componente | Tecnología | Estado |
|---|---|---|
| Orquestación | Apache Airflow (Helm) | reusa base · DAG nuevo |
| RAW / CLEAN DATA | PostgreSQL 15 (`raw_properties` / `clean_properties`) | esquema nuevo |
| Object storage (artefactos) | MinIO | reusa base |
| ML Tracking / Registry | MLflow 2.22.0 (backend Postgres) | reusa base |
| API de inferencia | FastAPI + Uvicorn | reescrita (regresión + `/reload`) |
| Interfaz | Streamlit (inferencia + historial) | reescrita |
| Observabilidad | Prometheus + Grafana | reusa base |
| Pruebas de carga | Locust 2.24.0 | reescrita |
| CI | GitHub Actions → DockerHub | **nuevo** (`.github/workflows/`) |
| GitOps | Argo CD | **nuevo** (`argocd/`) |

---

## Flujo del DAG (`dags/realty_pipeline.py`)

19 tareas con **dos bifurcaciones explícitas**:

```
start → fetch_batch_from_api → store_raw_batch → validate_schema →
validate_data_quality → detect_new_categories → detect_data_drift →
preprocess_data → decide_training ──┬─→ skip_training ─────────────────────┐
                                    └─→ train_candidate_model →            │
                                        evaluate_candidate_model →         │
                                        register_candidate_in_mlflow →     │
                                        compare_with_production →          │
                                        decide_promotion ─┬→ promote_model →┤
                                                          └→ reject_model ─→┤
                                                          notify_or_log_result → end
```

- **`decide_training`** (RF4): entrena solo si hay drift / nuevas categorías frecuentes /
  crecimiento de volumen / degradación — no por periodicidad.
- **`decide_promotion`** (RF6): promueve solo bajo la regla de MAE/RMSE.
- Cada lote queda registrado en la tabla `training_audit`, fuente del historial en Streamlit.

---

## Datos (RF2)

| Tabla | Rol |
|---|---|
| `raw_properties` | Lotes tal como llegan de la API + metadatos de ingestión (RF1/RF2) |
| `clean_properties` | Datos transformados y listos para entrenar, trazables al lote crudo |
| `training_audit` | Decisión por lote: validaciones, drift, entrenó/no, promovió/no, métricas (RF4/RF9) |
| `inference_logs` | Registro de cada inferencia: entrada, `predicted_price`, versión de modelo (RF8) |

---

## API de inferencia (RF7/RF8)

MLflow es la **única fuente de verdad**: el modelo se carga por alias `models:/realty-champion@champion`.

| Endpoint | Método | Descripción |
|---|---|---|
| `/health` | GET | Estado de la API y del modelo cargado |
| `/predict` | POST | Estima el precio de una propiedad |
| `/model-info` | GET | Nombre, versión y alias del modelo activo |
| `/reload` | POST | **Recarga el modelo desde MLflow sin redesplegar** (admin, token) |
| `/metrics` | GET | Métricas Prometheus |

La recarga es atómica con fallback al modelo previo si la descarga falla (ver `ModelCache` en `src/api/main.py`).

---

## Despliegue (GitOps con Argo CD)

> Kubernetes consume imágenes **desde DockerHub** (construidas por GitHub Actions). No se construyen
> imágenes en la máquina de despliegue ni se usa `kubectl apply` manual como mecanismo principal.

```bash
# Clúster local
minikube start --cpus 4 --memory 8192 --addons ingress,metrics-server

# Instalar Argo CD
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Registrar la aplicación (a partir de aquí Argo CD sincroniza Git → clúster)
kubectl apply -n argocd -f argocd/application.yaml
```

Secrets (`k8s/secrets.yaml`) y credenciales se gestionan fuera del código (RF seguridad). Falta
configurar en GitHub los secrets `DOCKERHUB_USERNAME` y `DOCKERHUB_TOKEN` para el workflow de CI.

---

## Acceso a los servicios (NodePort)

| Servicio | NodePort |
|---|---|
| Airflow UI | 30088 |
| MLflow UI | 30500 |
| MinIO Console | 30900 |
| FastAPI | 30800 |
| Streamlit | 30801 |
| Prometheus | 30909 |
| Grafana | 30300 |
| Locust | 30089 |

> Cada servicio vive en su propio namespace (ver tabla abajo). Ej.:
> `minikube service realty-api-svc -n api --url`, `... realty-ui-svc -n streamlit`, `... grafana-svc -n grafana`.

---

## Estructura del repositorio

```
.
├── .github/workflows/      # CI: build & push de imágenes a DockerHub (nuevo)
├── argocd/                 # Application de Argo CD (nuevo)
├── dags/
│   └── realty_pipeline.py  # DAG con bifurcaciones (decisión de entrenamiento y promoción)
├── src/
│   ├── ingestion/          # cliente robusto de la API de datos (nuevo)
│   ├── api/                # FastAPI de inferencia (regresión + recarga RF7)
│   └── ui/                 # Streamlit (inferencia + historial)
├── docker/                 # Dockerfiles (api, ui, mlflow, airflow)
├── k8s/                    # Manifiestos: postgres, minio, mlflow, airflow, api, ui, locust, observabilidad
├── migrations/             # Esquema SQL (raw/clean/audit/inference)
├── locust/                 # Prueba de carga
└── README.md
```

---

## Estado actual (~80%)

**Funcionando y validado end-to-end:**
- Pipeline completo en Airflow (DAG de 19 tareas, 2 bifurcaciones): ingesta por lotes → validación
  (esquema, calidad, categorías nuevas, drift KS) → decisión de entrenamiento (RF4) → entrenamiento
  + registro en MLflow (RF5) → comparación contra productivo + promoción condicionada (RF6).
  Demostrados los 3 caminos: **entrenó+promovió**, **entrenó+rechazó**, **no-entrenó**.
- MLflow (Postgres + MinIO), FastAPI con **recarga sin redespliegue** (RF7) y registro de inferencias (RF8),
  Streamlit (inferencia + historial).
- CI/CD: GitHub Actions → DockerHub (4 imágenes).
- Kubernetes en minikube: **un namespace por servicio** (10 namespaces), DNS cross-namespace, probes y recursos.
- Observabilidad: Prometheus, Grafana y Locust desplegados.

**Pendiente:**
- Argo CD (GitOps) — sincronización declarativa.
- Evidencia de prueba de carga Locust → Grafana (RF10).
- Documentación final y video de sustentación.

| Fase | Estado |
|---|---|
| 0 — Contrato de la API de datos | ✅ |
| 1 — DAG end-to-end (local) | ✅ |
| 2 — API/UI contra MLflow | ✅ |
| 3 — Despliegue en minikube (multi-namespace) | ✅ |
| 4 — Argo CD + observabilidad + docs + video | 🔜 en curso |

---

## Decisiones de diseño

- **Regresión con `TransformedTargetRegressor(log1p)`**: el modelo es un `Pipeline` de scikit-learn que
  incluye su propio preprocesamiento (imputación + `OrdinalEncoder` con `handle_unknown`). Así la API
  envía **features crudas** y el modelo hace todo internamente — clave para RF7 (no se quema preprocesamiento
  en la API). Se entrena sobre `log(price)` y se predice en escala de precio.
- **Métrica prioritaria: MAE** (con RMSE/MAPE/R² de apoyo). Regla de promoción: promover solo si
  **MAE baja ≥ 3 % y RMSE no empeora > 1 %**. MAPE se reporta pero no se usa como criterio porque hay
  precios muy bajos que lo inflan.
- **Tabla `training_audit` como fuente de verdad del historial**: cada lote escribe una fila (decisión,
  razón, drift, métricas, promoción). Es lo que lee Streamlit (RF9) y desacopla las tareas del DAG.
- **Categóricas como texto en CLEAN** (no pre-codificadas): el encoding vive en el `Pipeline` del modelo,
  manteniendo coherencia entre entrenamiento e inferencia.
- **Un namespace por servicio** (10 ns) con Secret replicado y DNS FQDN (`svc.namespace`), por recomendación
  del docente (estilo Proyecto 3 ampliado).
- **Airflow vía Helm chart oficial** (no manifiestos crudos): Airflow 3.x requiere api-server + scheduler +
  dag-processor + triggerer; el chart los gestiona. DAGs y código del pipeline **horneados en la imagen**
  (self-contained, sin gitSync/PVC).
- **`mlflow-skinny` en la imagen de Airflow**: las tareas solo necesitan el cliente de tracking/registry,
  no el servidor; evita el choque de dependencias (Flask/SQLAlchemy) con Airflow.
- **CI como fuente de imágenes**: Kubernetes consume de DockerHub; no se construyen imágenes en la máquina
  de despliegue.

---

## Problemas encontrados y soluciones

| Problema | Causa | Solución |
|---|---|---|
| Migración de Airflow fallaba (`DeclarativeBase`) | Instalar mlflow/sklearn sin las *constraints* de Airflow rompía SQLAlchemy | Imagen con `--constraint` oficial de Airflow + `mlflow-skinny` + base `3.0.6-python3.12` |
| Conflicto `packaging<25` vs constraints | mlflow pide `<25`, Airflow fija `==25` | Relajar solo esa línea de las constraints (`sed`) |
| api-server de Airflow en CrashLoop | Con varios workers de uvicorn los primeros mueren | `AIRFLOW__API__WORKERS=1` |
| `DagBag import timeout` al ejecutar tareas | El DAG importa sklearn/mlflow (pesado) | Subir `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120` |
| API no cargaba el modelo (`No module named '_loss'`) | sklearn distinto entre entrenamiento (1.7.1) e inferencia (1.4.2) | Alinear **scikit-learn 1.7.1** en ambas imágenes |
| PVCs en `Pending` | Manifiestos pedían `storageClassName: local-path` (de k3s) | Quitarlo → usar `standard` (default de minikube) |
| `data-api` y tareas con `OOMKilled` | Lotes muy grandes (hasta ~360k filas) en nodo de 6 GB | Subir Docker/WSL a 10 GB, minikube a 8 GB, `data-api`/scheduler a 3 GB, y **cap de 60k filas** en el fit |
| `minikube image load :latest` no refrescaba | Tag `latest` cacheado en el nodo | Usar tag único o `imagePullPolicy`/CI |

---

## Video de sustentación

_(pendiente — se publicará en YouTube, máx. 10 min)_
