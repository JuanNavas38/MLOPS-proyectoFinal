"""
Interfaz Streamlit — MLOps Proyecto Final (Nivel 4)
RF9: dos secciones obligatorias.
  1) Inferencia: formulario -> FastAPI -> precio estimado + versión de modelo.
  2) Historial: lee training_audit (decisión por lote, promoción, métricas).
"""

import os
import requests
import pandas as pd
import streamlit as st

API_URL = os.getenv("API_URL", "http://realty-api-svc:8000")
DB_URL  = os.getenv("DATABASE_URL", "postgresql://mlops:mlops2026@postgres-svc:5432/mlops")

st.set_page_config(page_title="Realty Price Predictor", page_icon="🏠", layout="wide")
st.title("🏠 Realty Price Predictor")
st.markdown("**MLOps Proyecto Final — Nivel 4 · Pontificia Universidad Javeriana**")

# Estado del modelo en la barra lateral
try:
    info = requests.get(f"{API_URL}/model-info", timeout=5).json()
    st.sidebar.success("Modelo activo")
    st.sidebar.json(info)
except Exception:
    st.sidebar.error("API no disponible")

tab_infer, tab_history = st.tabs(["🔮 Inferencia", "📜 Historial de entrenamiento"])

# ── 1) INFERENCIA ───────────────────────────────────────────────────────────────
with tab_infer:
    st.subheader("Estimar precio de una propiedad")
    col1, col2, col3 = st.columns(3)
    with col1:
        bed        = st.number_input("Habitaciones (bed)", 0, 20, 3)
        bath       = st.number_input("Baños (bath)", 0, 20, 2)
        house_size = st.number_input("Área habitable (sqft)", 0.0, 50000.0, 1500.0)
        acre_lot   = st.number_input("Terreno (acres)", 0.0, 1000.0, 0.25)
    with col2:
        status     = st.selectbox("Estado", ["for_sale", "ready_to_build"])
        city       = st.text_input("Ciudad (city)", "Adjuntas")
        state      = st.text_input("Estado/región (state)", "Puerto Rico")
        zip_code   = st.text_input("Código postal (zip_code)", "00601")
    with col3:
        brokered_by    = st.text_input("Corredor (brokered_by, opcional)", "")
        street         = st.text_input("Calle (street, opcional)", "")
        prev_sold_date = st.text_input("Última venta (YYYY-MM-DD, opcional)", "")

    if st.button("🔮 Estimar precio", type="primary", use_container_width=True):
        payload = {
            "brokered_by": brokered_by or None,
            "status": status,
            "bed": int(bed), "bath": int(bath),
            "acre_lot": float(acre_lot),
            "street": street or None,
            "city": city, "state": state, "zip_code": zip_code,
            "house_size": float(house_size),
            "prev_sold_date": prev_sold_date or None,
        }
        try:
            with st.spinner("Consultando modelo..."):
                resp = requests.post(f"{API_URL}/predict", json=payload, timeout=10)
                resp.raise_for_status()
                result = resp.json()
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Precio estimado", f"${result['predicted_price']:,.0f}")
            with c2:
                st.caption(f"Modelo: {result['model_name']} v{result['model_version']} "
                           f"(@{result['model_alias']})")
                st.caption(f"Latencia: {result['response_time_ms']:.1f} ms")
                st.caption(f"Request ID: {result['request_id']}")
        except requests.exceptions.ConnectionError:
            st.error("No se puede conectar con la API")
        except Exception as e:
            st.error(f"Error: {e}")

# ── 2) HISTORIAL ─────────────────────────────────────────────────────────────────
with tab_history:
    st.subheader("Historial de decisiones por lote")
    st.caption("Fuente: tabla training_audit (RF4/RF9).")
    try:
        df = pd.read_sql(
            """
            SELECT batch_id, executed_at, n_records_batch, drift_detected,
                   decision, decision_reason, promoted, promotion_reason,
                   candidate_metrics, production_metrics, model_version
            FROM training_audit
            ORDER BY executed_at DESC
            """,
            DB_URL,
        )
        if df.empty:
            st.info("Aún no hay lotes procesados.")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.warning(f"No se pudo leer el historial: {e}")
