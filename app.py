# app.py (Branded + Dark/Light + Extra KPIs, limpio sin emojis)
import os
from datetime import datetime, date
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from mysql.connector import pooling
import plotly.express as px
import plotly.io as pio
import base64

# =====================
# App Config
# =====================
st.set_page_config(
    page_title="Aging AP – KPIs",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================
# Load .env (local fallback)
# =====================
load_dotenv()

def get_secret(key: str, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)

# ===== DB & Branding desde secrets/env =====
DB_HOST = get_secret("DB_HOST", "")
DB_PORT = int(get_secret("DB_PORT", "3306"))
DB_USER = get_secret("DB_USER", "")
DB_PASS = get_secret("DB_PASS", "")
DB_NAME = get_secret("DB_NAME", "")

BRAND_NAME = get_secret("BRAND_NAME", "Brandatta")
LOGO_PATH  = get_secret("LOGO_PATH", "")
PRIMARY    = get_secret("PRIMARY_COLOR", "#0ea5e9")

# =====================
# Theming (runtime toggle)
# =====================
dark_mode = st.sidebar.toggle("Modo oscuro", value=False)
pio.templates.default = "plotly_dark" if dark_mode else "plotly_white"

def apply_css(primary_hex: str, dark: bool):
    text = "#e5e7eb" if dark else "#111827"
    bg = "#0b1220" if dark else "#ffffff"
    sbg = "#111827" if dark else "#f3f4f6"
    css = f"""
    <style>
    .stApp {{ background: {bg}; color: {text}; }}
    .brandbar {{
        display:flex; align-items:center; gap:.8rem; padding:.6rem 1rem;
        border-radius: 1rem;
        background: rgba(14,165,233,0.08);
        border:1px solid rgba(14,165,233,0.20);
        margin-bottom: .5rem;
    }}
    .brand-title {{ font-weight:700; font-size:1.05rem; letter-spacing:.2px; }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

apply_css(PRIMARY, dark_mode)

# =====================
# Branding Header
# =====================
def load_logo_b64(path: str):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except:
        return None

logo_b64 = load_logo_b64(LOGO_PATH)

colb1, colb2 = st.columns([1, 8])
with colb1:
    if logo_b64:
        st.markdown(
            f'<div class="brandbar"><img src="data:image/png;base64,{logo_b64}" height="40"/></div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'<div class="brandbar"><span class="brand-title">{BRAND_NAME} • Aging AP KPIs</span></div>',
            unsafe_allow_html=True
        )

with colb2:
    st.title("Aging de Cuentas a Pagar – KPIs")

# =====================
# DB Connection
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
def run_query(sql: str, params: tuple = ()):
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
    sql = """
        SELECT Sociedad, Proveedor, Proveedor_Nombre,
               `A Vencer`, `0-15`, `16-60`, `61-90`, `91-120`, `+120`, `Sin Vto`,
               Total, Total_MM
        FROM vista_aging_ap_resumen
        WHERE 1=1
    """
    params = []
    if sociedad:
        sql += " AND Sociedad = %s"
        params.append(sociedad)
    if proveedor:
        sql += " AND Proveedor = %s"
        params.append(proveedor)
    return run_query(sql, tuple(params))

def load_detalle(sociedad=None, proveedor=None):
    sql = """
        SELECT Sociedad, Proveedor, Proveedor_Nombre, Nro_Documento,
               Fecha_Factura, VtoSAP, ImpMonLoc, MonDoc, overdue_days, bucket
        FROM vista_aging_ap_detalle
        WHERE 1=1
    """
    params = []
    if sociedad:
        sql += " AND Sociedad = %s"
        params.append(sociedad)
    if proveedor:
        sql += " AND Proveedor = %s"
        params.append(proveedor)
    return run_query(sql + " ORDER BY VtoSAP ASC, ImpMonLoc DESC", tuple(params))

def load_sociedades():
    df = run_query("SELECT DISTINCT Sociedad FROM vista_aging_ap_resumen ORDER BY Sociedad")
    return df["Sociedad"].tolist() if not df.empty else []

def load_proveedores(sociedad=None):
    if sociedad:
        df = run_query(
            "SELECT DISTINCT Proveedor, Proveedor_Nombre FROM vista_aging_ap_resumen WHERE Sociedad=%s ORDER BY Proveedor_Nombre",
            (sociedad,)
        )
    else:
        df = run_query(
            "SELECT DISTINCT Proveedor, Proveedor_Nombre FROM vista_aging_ap_resumen ORDER BY Proveedor_Nombre"
        )
    return df if not df.empty else pd.DataFrame(columns=["Proveedor", "Proveedor_Nombre"])

# =====================
# Sidebar Filters
# =====================
with st.sidebar:
    st.header("Filtros")

    sociedades = load_sociedades()
    sociedad = st.selectbox("Sociedad", ["(Todas)"] + sociedades)
    sociedad = None if sociedad == "(Todas)" else sociedad

    prov_df = load_proveedores(sociedad)
    prov_map = {"(Todos)": None}
    for _, r in prov_df.iterrows():
        prov_map[f"{r['Proveedor_Nombre']} ({r['Proveedor']})"] = r["Proveedor"]

    proveedor = prov_map[st.selectbox("Proveedor", list(prov_map.keys()))]

# =====================
# Data
# =====================
resumen = load_resumen(sociedad, proveedor)
detalle = load_detalle(sociedad, proveedor)

def int0(x):
    try:
        return int(x)
    except:
        return 0

# =====================
# KPIs
# =====================
col = st.columns(10)
if resumen.empty:
    for c in col:
        c.metric("", "0")
else:
    sums = resumen[
        ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto","Total","Total_MM"]
    ].sum()

    overdue = int0(sums["0-15"] + sums["16-60"] + sums["61-90"] + sums["91-120"] + sums["+120"])
    total = int0(sums["Total"])
    total_mm = int0(sums["Total_MM"])
    porc_overdue = round(overdue / total * 100, 1) if total else 0
    docs = len(detalle) if not detalle.empty else resumen.shape[0]

    labels = ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto","Total MM (-1)","% Vencido","Docs"]
    values = [
        int0(sums["A Vencer"]), int0(sums["0-15"]), int0(sums["16-60"]),
        int0(sums["61-90"]), int0(sums["91-120"]), int0(sums["+120"]),
        int0(sums["Sin Vto"]), total_mm, porc_overdue, docs
    ]

    for c, l, v in zip(col, labels, values):
        c.metric(l, f"{v:,}".replace(",", "."))

# =====================
# Visuals
# =====================
st.divider()

if not resumen.empty:
    buckets = ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]
    buck_vals = {b: int0(resumen[b].sum()) for b in buckets}

    df = pd.DataFrame({"Bucket": buckets, "Importe": [buck_vals[b] for b in buckets]})

    colA, colB = st.columns([2,1])

    with colA:
        fig = px.bar(df, x="Bucket", y="Importe", text="Importe")
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with colB:
        pie_df = pd.DataFrame({
            "Estado": ["Vencido", "A Vencer", "Sin Vto"],
            "Importe": [
                buck_vals["0-15"] + buck_vals["16-60"] + buck_vals["61-90"] + buck_vals["91-120"] + buck_vals["+120"],
                buck_vals["A Vencer"],
                buck_vals["Sin Vto"]
            ]
        })
        fig2 = px.pie(pie_df, names="Estado", values="Importe")
        st.plotly_chart(fig2, use_container_width=True)

    top = resumen.groupby("Proveedor_Nombre")["Total"].sum().reset_index().sort_values("Total", ascending=False).head(20)
    fig3 = px.treemap(top, path=["Proveedor_Nombre"], values="Total")
    st.plotly_chart(fig3, use_container_width=True)

# =====================
# Detail Table
# =====================
st.subheader("Detalle de Comprobantes")
if detalle.empty:
    st.write("Sin datos para los filtros seleccionados.")
else:
    det = detalle.copy()
    det["Fecha_Factura"] = pd.to_datetime(det["Fecha_Factura"]).dt.date
    det["VtoSAP"] = pd.to_datetime(det["VtoSAP"]).dt.date
    st.dataframe(det, use_container_width=True, height=420)

    colD1, colD2 = st.columns(2)
    with colD1:
        st.download_button("Descargar Resumen (CSV)", resumen.to_csv(index=False), "resumen.csv")
    with colD2:
        st.download_button("Descargar Detalle (CSV)", det.to_csv(index=False), "detalle.csv")
