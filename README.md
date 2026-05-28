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

> En minikube: `minikube service <svc> -n mlops --url` o `kubectl port-forward`.

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

## Pendientes (roadmap por fases)

1. **Fase 0** — levantar la API de datos local e inspeccionar su contrato real.
2. **Fase 1** — implementar el DAG (cliente API, validaciones, drift, preprocesamiento, entrenamiento, comparación/promoción).
3. **Fase 2** — completar API/UI y dejar el flujo end-to-end funcionando local.
4. **Fase 3** — desplegar en minikube con recursos/probes.
5. **Fase 4** — CI (GitHub Actions → DockerHub) + Argo CD + documentación + video.

---

## Video de sustentación

_(pendiente — se publicará en YouTube, máx. 10 min)_
