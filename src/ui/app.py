import streamlit as st
import requests
import json

API_URL = "http://diabetes-api-svc:8000"

st.set_page_config(
    page_title="Diabetes Readmission Predictor",
    page_icon="🏥",
    layout="wide"
)

st.title("Diabetes Readmission Predictor")
st.markdown("**MLOps Proyecto 2 — Pontificia Universidad Javeriana**")

# ── Model info ────────────────────────────────────────────────────────────────
try:
    info = requests.get(f"{API_URL}/model-info", timeout=5).json()
    st.sidebar.success(f"Modelo activo")
    st.sidebar.json(info)
except:
    st.sidebar.error("API no disponible")

st.markdown("---")

# ── Ejemplo rápido ────────────────────────────────────────────────────────────
EXAMPLE = {
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
    "service_utilization": 3,
}

if st.button("Cargar valores de ejemplo"):
    st.session_state.update(EXAMPLE)

# ── Formulario ────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Estancia y procedimientos")
    time_in_hospital    = st.slider("Días en hospital", 1, 14,
                                     st.session_state.get("time_in_hospital", 3))
    num_lab_procedures  = st.number_input("Procedimientos de laboratorio", 0, 200,
                                           st.session_state.get("num_lab_procedures", 40))
    num_procedures      = st.number_input("Procedimientos médicos", 0, 10,
                                           st.session_state.get("num_procedures", 1))
    num_medications     = st.number_input("Medicamentos", 0, 100,
                                           st.session_state.get("num_medications", 10))
    number_diagnoses    = st.number_input("Número de diagnósticos", 0, 20,
                                           st.session_state.get("number_diagnoses", 5))

with col2:
    st.subheader("Historial de visitas")
    number_outpatient   = st.number_input("Visitas ambulatorias (año anterior)", 0, 50,
                                           st.session_state.get("number_outpatient", 0))
    number_emergency    = st.number_input("Visitas emergencia (año anterior)", 0, 50,
                                           st.session_state.get("number_emergency", 0))
    number_inpatient    = st.number_input("Hospitalizaciones (año anterior)", 0, 20,
                                           st.session_state.get("number_inpatient", 0))
    service_utilization = number_outpatient + number_emergency + number_inpatient

with col3:
    st.subheader("Características del paciente")
    age_labels = ["0-10","10-20","20-30","30-40","40-50","50-60","60-70","70-80","80-90","90-100"]
    age_encoded = st.selectbox("Rango de edad",
                                range(10), index=st.session_state.get("age_encoded", 6),
                                format_func=lambda x: age_labels[x])
    admission_type_encoded   = st.number_input("Tipo de admisión (encoded)", 0, 8,
                                                st.session_state.get("admission_type_encoded", 1))
    discharge_encoded        = st.number_input("Tipo de alta (encoded)", 0, 30,
                                                st.session_state.get("discharge_encoded", 1))
    admission_source_encoded = st.number_input("Fuente de admisión (encoded)", 0, 25,
                                                st.session_state.get("admission_source_encoded", 1))
    insulin_encoded          = st.number_input("Insulina (encoded)", 0, 3,
                                                st.session_state.get("insulin_encoded", 0))
    change_encoded           = st.number_input("Cambio de medicación (encoded)", 0, 1,
                                                st.session_state.get("change_encoded", 0))
    diabetesmed_encoded      = st.number_input("Medicación diabetes (encoded)", 0, 1,
                                                st.session_state.get("diabetesmed_encoded", 1))
    a1cresult_encoded        = st.number_input("Resultado A1C (encoded)", 0, 3,
                                                st.session_state.get("a1cresult_encoded", 0))
    max_glu_serum_encoded    = st.number_input("Glucosa sérica (encoded)", 0, 3,
                                                st.session_state.get("max_glu_serum_encoded", 0))

num_medications_log = float(__import__("math").log1p(num_medications))

# ── Predicción ────────────────────────────────────────────────────────────────
st.markdown("---")
if st.button("🔮 Predecir readmisión", type="primary", use_container_width=True):
    payload = {
        "time_in_hospital":         int(time_in_hospital),
        "num_lab_procedures":       int(num_lab_procedures),
        "num_procedures":           int(num_procedures),
        "num_medications":          int(num_medications),
        "number_outpatient":        int(number_outpatient),
        "number_emergency":         int(number_emergency),
        "number_inpatient":         int(number_inpatient),
        "number_diagnoses":         int(number_diagnoses),
        "age_encoded":              int(age_encoded),
        "admission_type_encoded":   int(admission_type_encoded),
        "discharge_encoded":        int(discharge_encoded),
        "admission_source_encoded": int(admission_source_encoded),
        "insulin_encoded":          int(insulin_encoded),
        "change_encoded":           int(change_encoded),
        "diabetesmed_encoded":      int(diabetesmed_encoded),
        "a1cresult_encoded":        int(a1cresult_encoded),
        "max_glu_serum_encoded":    int(max_glu_serum_encoded),
        "num_medications_log":      num_medications_log,
        "service_utilization":      int(service_utilization),
    }

    try:
        with st.spinner("Consultando modelo..."):
            resp = requests.post(f"{API_URL}/predict",
                                  json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()

        col_r1, col_r2, col_r3 = st.columns(3)
        with col_r1:
            if result["prediction"] == 1:
                st.error("**READMISIÓN TEMPRANA** (<30 días)")
            else:
                st.success("**SIN READMISIÓN TEMPRANA**")

        with col_r2:
            prob1 = result["probability_class1"]
            st.metric("Probabilidad de readmisión", f"{prob1:.1%}")
            st.progress(prob1)

        with col_r3:
            st.metric("Tiempo de respuesta", f"{result['response_time_ms']:.1f} ms")
            st.caption(f"Modelo: {result['model_name']} v{result['model_version']}")
            st.caption(f"Request ID: {result['request_id']}")

    except requests.exceptions.ConnectionError:
        st.error("No se puede conectar con la API")
    except Exception as e:
        st.error(f"Error: {str(e)}")