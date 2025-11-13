import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from mysql.connector import pooling
import plotly.express as px
import plotly.io as pio

# =====================================================================================
# CONFIG
# =====================================================================================
st.set_page_config(
    page_title="Aging AP – KPIs",
    page_icon="",
    layout="wide"
)

# =====================================================================================
# LOAD SECRETS / ENV
# =====================================================================================
load_dotenv()

def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except:
        return os.getenv(key, default)

DB_HOST = get_secret("DB_HOST")
DB_PORT = int(get_secret("DB_PORT", "3306"))
DB_USER = get_secret("DB_USER")
DB_PASS = get_secret("DB_PASS")
DB_NAME = get_secret("DB_NAME")

# =====================================================================================
# DB CONNECTION
# =====================================================================================
pool = pooling.MySQLConnectionPool(
    pool_name="pool",
    pool_size=5,
    pool_reset_session=True,
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)

@st.cache_data(ttl=300)
def run_query(sql, params=()):
    conn = pool.get_connection()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    finally:
        conn.close()

# =====================================================================================
# LOADERS
# =====================================================================================
def load_resumen(soc=None, prov=None):
    sql = """
    SELECT Sociedad, Proveedor, Proveedor_Nombre,
        `A Vencer`, `0-15`, `16-60`, `61-90`, `91-120`, `+120`, `Sin Vto`,
        Total, Total_MM
    FROM vista_aging_ap_resumen
    WHERE 1=1
    """
    params = []
    if soc:
        sql += " AND Sociedad=%s"
        params.append(soc)
    if prov:
        sql += " AND Proveedor=%s"
        params.append(prov)
    return run_query(sql, tuple(params))

def load_detalle(soc=None, prov=None):
    sql = """
    SELECT Sociedad, Proveedor, Proveedor_Nombre, Nro_Documento,
           Fecha_Factura, VtoSAP, ImpMonLoc, MonDoc, overdue_days, bucket
    FROM vista_aging_ap_detalle
    WHERE 1=1
    """
    params = []
    if soc:
        sql += " AND Sociedad=%s"
        params.append(soc)
    if prov:
        sql += " AND Proveedor=%s"
        params.append(prov)
    sql += " ORDER BY VtoSAP ASC, ImpMonLoc DESC"
    return run_query(sql, tuple(params))

def load_sociedades():
    df = run_query("SELECT DISTINCT Sociedad FROM vista_aging_ap_resumen ORDER BY Sociedad")
    return df["Sociedad"].tolist() if not df.empty else []

def load_proveedores(soc=None):
    if soc:
        df = run_query("""
            SELECT DISTINCT Proveedor, Proveedor_Nombre
            FROM vista_aging_ap_resumen
            WHERE Sociedad=%s
            ORDER BY Proveedor_Nombre
        """, (soc,))
    else:
        df = run_query("""
            SELECT DISTINCT Proveedor, Proveedor_Nombre
            FROM vista_aging_ap_resumen
            ORDER BY Proveedor_Nombre
        """)
    return df

# =====================================================================================
# UI
# =====================================================================================

st.title("Aging de Cuentas a Pagar – KPIs")

# Sidebar
with st.sidebar:
    st.header("Filtros")

    sociedades = load_sociedades()
    sociedad = st.selectbox("Sociedad", ["(Todas)"] + sociedades)
    sociedad = None if sociedad == "(Todas)" else sociedad

    prov_df = load_proveedores(sociedad)
    prov_map = {"(Todos)": None}
    for _, r in prov_df.iterrows():
        prov_map[f"{r['Proveedor_Nombre']} ({r['Proveedor']})"] = r["Proveedor"]

    proveedor_label = st.selectbox("Proveedor", list(prov_map.keys()))
    proveedor = prov_map[proveedor_label]

# =====================================================================================
# DATA
# =====================================================================================
resumen = load_resumen(sociedad, proveedor)
detalle = load_detalle(sociedad, proveedor)

def int0(x):
    try: return int(x)
    except: return 0

# =====================================================================================
# KPI BLOCK
# =====================================================================================
col = st.columns(10)

if resumen.empty:
    for c in col: c.metric("", "0")
else:
    sums = resumen[
        ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto","Total","Total_MM"]
    ].sum()

    overdue = int0(sums["0-15"] + sums["16-60"] + sums["61-90"] + sums["91-120"] + sums["+120"])
    total = int0(sums["Total"])
    total_mm = int0(sums["Total_MM"])
    porc = round(overdue/total*100, 1) if total else 0
    docs = len(detalle) if not detalle.empty else resumen.shape[0]

    vals = [
        int0(sums["A Vencer"]), int0(sums["0-15"]), int0(sums["16-60"]),
        int0(sums["61-90"]), int0(sums["91-120"]), int0(sums["+120"]),
        int0(sums["Sin Vto"]), total_mm, porc, docs
    ]
    labels = [
        "A Vencer","0-15","16-60","61-90","91-120","+120",
        "Sin Vto","Total MM (-1)","% Vencido","Docs"
    ]

    for c, l, v in zip(col, labels, vals):
        c.metric(l, f"{v:,}".replace(",", "."))

st.divider()

# =====================================================================================
# GRAPHS
# =====================================================================================
if not resumen.empty:
    buckets = ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]
    buck_vals = {b: int0(resumen[b].sum()) for b in buckets}

    df_b = pd.DataFrame({"Bucket": buckets, "Importe": [buck_vals[b] for b in buckets]})

    col1, col2 = st.columns([2,1])

    with col1:
        fig = px.bar(df_b, x="Bucket", y="Importe", text="Importe")
        fig.update_traces(texttemplate="%{text:,}")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        df_pie = pd.DataFrame({
            "Estado": ["Vencido","A Vencer","Sin Vto"],
            "Importe": [
                buck_vals["0-15"] + buck_vals["16-60"] + buck_vals["61-90"] +
                buck_vals["91-120"] + buck_vals["+120"],
                buck_vals["A Vencer"],
                buck_vals["Sin Vto"]
            ]
        })
        fig2 = px.pie(df_pie, names="Estado", values="Importe")
        st.plotly_chart(fig2, use_container_width=True)

    # Top proveedores
    top = (
        resumen.groupby("Proveedor_Nombre")["Total"]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
        .head(20)
    )
    fig3 = px.treemap(top, path=["Proveedor_Nombre"], values="Total")
    st.plotly_chart(fig3, use_container_width=True)

# =====================================================================================
# DETAIL TABLE
# =====================================================================================
st.subheader("Detalle de Comprobantes")

if detalle.empty:
    st.write("Sin datos para los filtros seleccionados.")
else:
    det = detalle.copy()
    det["Fecha_Factura"] = pd.to_datetime(det["Fecha_Factura"]).dt.date
    det["VtoSAP"] = pd.to_datetime(det["VtoSAP"]).dt.date

    st.dataframe(det, use_container_width=True, height=420)

    c1, c2 = st.columns(2)
    c1.download_button("Descargar resumen (CSV)", resumen.to_csv(index=False).encode(), "resumen.csv")
    c2.download_button("Descargar detalle (CSV)", det.to_csv(index=False).encode(), "detalle.csv")
