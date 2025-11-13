import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from mysql.connector import pooling
import plotly.express as px
from streamlit_plotly_events import plotly_events

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
    except Exception:
        return os.getenv(key, default)

DB_HOST = get_secret("DB_HOST")
DB_PORT = int(get_secret("DB_PORT", "3306"))
DB_USER = get_secret("DB_USER")
DB_PASS = get_secret("DB_PASS")
DB_NAME = get_secret("DB_NAME")

# estado seleccionado en el pie (para filtrar detalle)
if "estado_filter" not in st.session_state:
    st.session_state["estado_filter"] = None

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
    try:
        return int(x)
    except Exception:
        return 0

# =====================================================================================
# KPI BLOCK
# =====================================================================================
col = st.columns(10)

if resumen.empty:
    for c in col:
        c.metric("", "0")
else:
    sums = resumen[
        ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto","Total","Total_MM"]
    ].sum(numeric_only=True)

    # Valores contables (pueden ser negativos)
    a_vencer = int0(sums["A Vencer"])
    b_0_15   = int0(sums["0-15"])
    b_16_60  = int0(sums["16-60"])
    b_61_90  = int0(sums["61-90"])
    b_91_120 = int0(sums["91-120"])
    b_120p   = int0(sums["+120"])
    sin_vto  = int0(sums["Sin Vto"])
    total    = int0(sums["Total"])
    total_mm = int0(sums["Total_MM"])

    overdue_raw = b_0_15 + b_16_60 + b_61_90 + b_91_120 + b_120p

    # Para porcentajes trabajamos con valores absolutos
    total_abs   = abs(total)
    overdue_abs = abs(overdue_raw)
    porc        = round(overdue_abs / total_abs * 100, 1) if total_abs else 0

    docs = len(detalle) if not detalle.empty else resumen.shape[0]

    vals = [
        a_vencer, b_0_15, b_16_60,
        b_61_90, b_91_120, b_120p,
        sin_vto, total_mm, porc, docs
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

    # Bar chart (por bucket, en absoluto)
    df_b = pd.DataFrame({
        "Bucket": buckets,
        "Importe": [abs(buck_vals[b]) for b in buckets]
    })

    col1, col2 = st.columns([2,1])

    with col1:
        fig = px.bar(
            df_b,
            x="Bucket",
            y="Importe",
            text="Importe",
            title="Distribución por bucket"
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # Pie chart basado en DETALLE (agrupado a Vencido / A Vencer / Sin Vto)
    with col2:
        if not detalle.empty:
            det_pie = detalle.copy()

            def map_estado(bucket):
                if bucket == "A Vencer":
                    return "A Vencer"
                elif bucket == "Sin Vto":
                    return "Sin Vto"
                else:
                    return "Vencido"

            det_pie["Estado"] = det_pie["bucket"].apply(map_estado)
            det_pie["importe_abs"] = det_pie["ImpMonLoc"].abs()

            df_pie = (
                det_pie.groupby("Estado", as_index=False)["importe_abs"]
                .sum()
                .rename(columns={"importe_abs": "Importe"})
            )

            fig2 = px.pie(
                df_pie,
                names="Estado",
                values="Importe",
                title="Vencido vs A Vencer"
            )

            selected = plotly_events(
                fig2,
                click_event=True,
                hover_event=False,
                select_event=False,
                key="pie_estado"
            )

            if selected:
                estado_click = selected[0].get("label") or selected[0].get("Estado")
                # toggle: si clickeo el mismo, saco el filtro
                if st.session_state["estado_filter"] == estado_click:
                    st.session_state["estado_filter"] = None
                else:
                    st.session_state["estado_filter"] = estado_click

            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.write("Sin datos de detalle para construir el gráfico Vencido vs A Vencer.")

    # Treemap: Top 20 proveedores (en absoluto)
    top = (
        resumen.groupby("Proveedor_Nombre")["Total"]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
        .head(20)
    )
    top["Total_abs"] = top["Total"].abs()
    fig3 = px.treemap(
        top,
        path=["Proveedor_Nombre"],
        values="Total_abs",
        title="Top 20 proveedores por exposición total"
    )
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
    det["VtoSAP"]        = pd.to_datetime(det["VtoSAP"]).dt.date

    estado = st.session_state.get("estado_filter")

    if estado == "Vencido":
        det_filtrado = det[det["bucket"].isin(["0-15","16-60","61-90","91-120","+120"])]
    elif estado == "A Vencer":
        det_filtrado = det[det["bucket"] == "A Vencer"]
    elif estado == "Sin Vto":
        det_filtrado = det[det["bucket"] == "Sin Vto"]
    else:
        det_filtrado = det

    st.dataframe(det_filtrado, use_container_width=True, height=420)

    total_regs = len(det_filtrado)
    if estado:
        st.write(f"Cantidad de comprobantes mostrados (filtro: {estado}): {total_regs}")
    else:
        st.write(f"Cantidad de comprobantes mostrados: {total_regs}")

    c1, c2 = st.columns(2)
    c1.download_button(
        "Descargar resumen (CSV)",
        resumen.to_csv(index=False).encode("utf-8"),
        "resumen.csv",
        mime="text/csv"
    )
    c2.download_button(
        "Descargar detalle (CSV)",
        det_filtrado.to_csv(index=False).encode("utf-8"),
        "detalle.csv",
        mime="text/csv"
    )
