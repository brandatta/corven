# app.py (Branded + Dark/Light + Extra KPIs, con st.secrets + .env fallback)
import os
from datetime import datetime, date
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling
import plotly.express as px
import plotly.io as pio
import base64

# =====================
# App Config
# =====================
st.set_page_config(
    page_title="Aging AP – KPIs",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================
# Load .env (local fallback)
# =====================
load_dotenv()

def get_secret(key: str, default=None):
    # 1) intenta desde st.secrets (Streamlit Cloud)
    try:
        return st.secrets[key]
    except Exception:
        # 2) fallback a variables de entorno / .env
        return os.getenv(key, default)

# ===== DB & Branding desde secrets/env =====
DB_HOST = get_secret("DB_HOST", "")
DB_PORT = int(get_secret("DB_PORT", "3306"))
DB_USER = get_secret("DB_USER", "")
DB_PASS = get_secret("DB_PASS", "")
DB_NAME = get_secret("DB_NAME", "")

BRAND_NAME = get_secret("BRAND_NAME", "Brandatta")
LOGO_PATH  = get_secret("LOGO_PATH", "")  # optional local path (PNG/SVG)
PRIMARY    = get_secret("PRIMARY_COLOR", "#0ea5e9")

# =====================
# Theming (runtime toggle)
# =====================
dark_mode = st.sidebar.toggle("🌙 Modo oscuro", value=False)
pio.templates.default = "plotly_dark" if dark_mode else "plotly_white"

def apply_css(primary_hex: str, dark: bool):
    text = "#e5e7eb" if dark else "#111827"
    bg = "#0b1220" if dark else "#ffffff"
    sbg = "#111827" if dark else "#f3f4f6"
    css = f"""
    <style>
    :root {{
        --primary: {primary_hex};
        --text: {text};
        --bg: {bg};
        --sbg: {sbg};
    }}
    .stApp {{ background: var(--bg); color: var(--text); }}
    .block-container {{ padding-top: 2rem; }}
    .kpi-card {{
        border-radius: 1rem; padding: 0.9rem 1rem; background: var(--sbg);
        border: 1px solid rgba(0,0,0,0.05);
    }}
    .brandbar {{
        display:flex; align-items:center; gap:.8rem; padding:.6rem 1rem;
        background: linear-gradient(90deg, rgba(14,165,233,0.12), rgba(14,165,233,0.02));
        border-radius: 1rem; border:1px solid rgba(14,165,233,0.25);
        margin-bottom: .5rem;
    }}
    .brand-title {{ font-weight: 700; font-size: 1.05rem; letter-spacing:.2px; }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

apply_css(PRIMARY, dark_mode)

# =====================
# Header with branding
# =====================
def load_logo_b64(path: str):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None

logo_b64 = load_logo_b64(LOGO_PATH)

colb1, colb2 = st.columns([1, 8])
with colb1:
    if logo_b64:
        st.markdown(
            f'<div class="brandbar"><img src="data:image/png;base64,{logo_b64}" height="40"/></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="brandbar"><span class="brand-title">📊 {BRAND_NAME} • Aging AP KPIs</span></div>',
            unsafe_allow_html=True,
        )

with colb2:
    st.title("Aging de Cuentas a Pagar – KPIs")
    st.caption("Impacto visual • Acceso instantáneo a los indicadores críticos")

# =====================
# Connection Pool
# =====================
pool = None
if DB_HOST and DB_NAME:
    pool = pooling.MySQLConnectionPool(
        pool_name="ap_pool",
        pool_size=5,
        pool_reset_session=True,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
    )

@st.cache_data(ttl=300, show_spinner=True)
def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    if pool is None:
        return pd.DataFrame()
    cnx = pool.get_connection()
    try:
        df = pd.read_sql(sql, cnx, params=params)
        return df
    finally:
        cnx.close()

# =====================
# Data Loaders
# =====================
def load_resumen(sociedad=None, proveedor=None):
    base_sql = """
        SELECT
            Sociedad, Proveedor, Proveedor_Nombre,
            `A Vencer`, `0-15`, `16-60`, `61-90`, `91-120`, `+120`, `Sin Vto`,
            Total, Total_MM
        FROM vista_aging_ap_resumen
        WHERE 1=1
    """
    params = []
    if sociedad:
        base_sql += " AND Sociedad = %s"
        params.append(sociedad)
    if proveedor:
        base_sql += " AND Proveedor = %s"
        params.append(proveedor)
    return run_query(base_sql, tuple(params))

def load_detalle(sociedad=None, proveedor=None):
    base_sql = """
        SELECT
            Sociedad, Proveedor, Proveedor_Nombre, Nro_Documento,
            Fecha_Factura, VtoSAP, ImpMonLoc, MonDoc, overdue_days, bucket
        FROM vista_aging_ap_detalle
        WHERE 1=1
    """
    params = []
    if sociedad:
        base_sql += " AND Sociedad = %s"
        params.append(sociedad)
    if proveedor:
        base_sql += " AND Proveedor = %s"
        params.append(proveedor)
    return run_query(base_sql + " ORDER BY VtoSAP ASC, ImpMonLoc DESC", tuple(params))

def load_sociedades():
    sql = "SELECT DISTINCT Sociedad FROM vista_aging_ap_resumen ORDER BY Sociedad"
    df = run_query(sql)
    return df["Sociedad"].dropna().tolist() if not df.empty else []

def load_proveedores(sociedad=None):
    if sociedad:
        sql = """
            SELECT DISTINCT Proveedor, Proveedor_Nombre
            FROM vista_aging_ap_resumen
            WHERE Sociedad = %s
            ORDER BY Proveedor_Nombre
        """
        df = run_query(sql, (sociedad,))
    else:
        sql = """
            SELECT DISTINCT Proveedor, Proveedor_Nombre
            FROM vista_aging_ap_resumen
            ORDER BY Proveedor_Nombre
        """
        df = run_query(sql)
    return df if not df.empty else pd.DataFrame(columns=["Proveedor", "Proveedor_Nombre"])

# =====================
# Sidebar Filters
# =====================
with st.sidebar:
    st.header("⚙️ Filtros")
    if pool is None:
        st.warning(
            "Configura las variables de conexión a la base de datos en los *secrets* de Streamlit o en .env",
            icon="⚠️",
        )

    sociedades = load_sociedades()
    sociedad = st.selectbox("Sociedad", ["(Todas)"] + sociedades, index=0)
    sociedad = None if sociedad == "(Todas)" else sociedad

    prov_df = load_proveedores(sociedad)
    prov_map = {"(Todos)": None}
    for _, row in prov_df.iterrows():
        prov_map[f"{row['Proveedor_Nombre']} ({row['Proveedor']})"] = row["Proveedor"]
    proveedor_label = st.selectbox("Proveedor", list(prov_map.keys()), index=0)
    proveedor = prov_map[proveedor_label]

    st.caption("💡 Tip: Filtros por 'Sociedad' y 'Proveedor' afectan todos los KPIs y visuales.")

# =====================
# Data
# =====================
resumen = load_resumen(sociedad, proveedor)
detalle = load_detalle(sociedad, proveedor)

def int0(x):
    try:
        return int(x)
    except Exception:
        return 0

# =====================
# KPI Row (extended)
# =====================
col = st.columns(10)
if resumen.empty:
    for c in col:
        with c:
            st.metric("—", "0")
else:
    sums = resumen[
        ["A Vencer", "0-15", "16-60", "61-90", "91-120", "+120", "Sin Vto", "Total", "Total_MM"]
    ].sum(numeric_only=True)

    overdue_total = int0(
        sums.get("0-15", 0)
        + sums.get("16-60", 0)
        + sums.get("61-90", 0)
        + sums.get("91-120", 0)
        + sums.get("+120", 0)
    )
    avencer_total = int0(sums.get("A Vencer", 0))
    sinvto_total = int0(sums.get("Sin Vto", 0))
    total = int0(sums.get("Total", 0))
    total_mm = int0(sums.get("Total_MM", 0))

    porc_overdue = (overdue_total / total * 100) if total else 0
    doc_count = len(detalle) if not detalle.empty else int(resumen.shape[0])
    buckets = {
        k: int0(sums.get(k, 0))
        for k in ["A Vencer", "0-15", "16-60", "61-90", "91-120", "+120", "Sin Vto"]
    }
    top_bucket = max(buckets, key=buckets.get) if buckets else "—"

    labels = [
        "A Vencer",
        "0-15",
        "16-60",
        "61-90",
        "91-120",
        "+120",
        "Sin Vto",
        "Total MM (-1)",
        "% Vencido",
        "Docs",
    ]
    values = [
        avencer_total,
        buckets["0-15"],
        buckets["16-60"],
        buckets["61-90"],
        buckets["91-120"],
        buckets["+120"],
        sinvto_total,
        total_mm,
        round(porc_overdue, 1),
        doc_count,
    ]

    for c, lab, val in zip(col, labels, values):
        with c:
            st.metric(lab, f"{val:,}".replace(",", "."))

    st.caption(f"📌 Bucket dominante: **{top_bucket}**")

st.markdown("---")

# =====================
# Visuals
# =====================
if resumen.empty:
    st.info("No hay datos para los filtros seleccionados.")
else:
    buckets_order = ["A Vencer", "0-15", "16-60", "61-90", "91-120", "+120", "Sin Vto"]
    buck_vals = {b: int0(resumen[b].sum()) for b in buckets_order if b in resumen.columns}

    buck_df = pd.DataFrame(
        {"Bucket": list(buck_vals.keys()), "Importe": list(buck_vals.values())}
    )
    buck_df["Bucket"] = pd.Categorical(
        buck_df["Bucket"], categories=buckets_order, ordered=True
    )
    buck_df = buck_df.sort_values("Bucket")

    colA, colB = st.columns([2, 1])
    with colA:
        fig_bar = px.bar(
            buck_df,
            x="Bucket",
            y="Importe",
            text="Importe",
            title="Distribución por Bucket",
        )
        fig_bar.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig_bar.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=420)
        st.plotly_chart(fig_bar, use_container_width=True)

    with colB:
        overdue_total = int0(
            buck_vals.get("0-15", 0)
            + buck_vals.get("16-60", 0)
            + buck_vals.get("61-90", 0)
            + buck_vals.get("91-120", 0)
            + buck_vals.get("+120", 0)
        )
        avencer_total = int0(buck_vals.get("A Vencer", 0))
        sinvto_total = int0(buck_vals.get("Sin Vto", 0))
        pie_df = pd.DataFrame(
            {
                "Estado": ["Vencido", "A Vencer", "Sin Vto"],
                "Importe": [overdue_total, avencer_total, sinvto_total],
            }
        )
        fig_pie = px.pie(
            pie_df,
            names="Estado",
            values="Importe",
            title="Vencido vs A Vencer",
        )
        fig_pie.update_traces(textposition="inside")
        fig_pie.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=420)
        st.plotly_chart(fig_pie, use_container_width=True)

    treemap_cols = ["Proveedor_Nombre", "Total"]
    if all(c in resumen.columns for c in treemap_cols):
        top = (
            resumen.groupby("Proveedor_Nombre", as_index=False)["Total"]
            .sum()
            .sort_values("Total", ascending=False)
            .head(20)
        )
        fig_tree = px.treemap(
            top,
            path=["Proveedor_Nombre"],
            values="Total",
            title="Top 20 Proveedores por Exposición (Total)",
        )
        fig_tree.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=450)
        st.plotly_chart(fig_tree, use_container_width=True)

st.markdown("### 📄 Detalle de Comprobantes")
if detalle.empty:
    st.caption("Sin detalle para los filtros actuales.")
else:
    det = detalle.copy()
    det["Fecha_Factura"] = pd.to_datetime(det["Fecha_Factura"], errors="coerce").dt.date
    det["VtoSAP"] = pd.to_datetime(det["VtoSAP"], errors="coerce").dt.date
    det = det.sort_values(by=["VtoSAP", "ImpMonLoc"], ascending=[True, False])
    st.dataframe(det, use_container_width=True, height=420)

    colDL1, colDL2 = st.columns(2)
    with colDL1:
        if not resumen.empty:
            csv = resumen.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Descargar resumen (CSV)",
                data=csv,
                file_name="aging_ap_resumen.csv",
                mime="text/csv",
            )
    with colDL2:
        if not det.empty:
            csv2 = det.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Descargar detalle (CSV)",
                data=csv2,
                file_name="aging_ap_detalle.csv",
                mime="text/csv",
            )

st.markdown("---")
with st.expander("🗣️ Modo presentación (guión sugerido)"):
    st.markdown(
        """
1) **Panorama general**: KPIs arriba, con foco en *Total MM (-1)* y *% Vencido*.
2) **Riesgo por bucket**: el gráfico de barras destaca dónde concentrar la acción.
3) **Prioridades**: treemap muestra *top 20 proveedores* por exposición.
4) **Accionable**: detalle ordenado por **vencimiento** y **importe** para ejecutar pagos.
"""
    )

st.caption(
    f"© {BRAND_NAME} • Demo de KPIs de Cuentas a Pagar – visual, rápida y lista para impresionar"
)
