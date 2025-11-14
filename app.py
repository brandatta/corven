import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from mysql.connector import pooling
import plotly.express as px

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
    sql = """
        SELECT Proveedor, Proveedor_Nombre, SUM(Total) AS Deuda
        FROM vista_aging_ap_resumen
        WHERE 1=1
    """
    params = []
    if soc:
        sql += " AND Sociedad=%s"
        params.append(soc)

    sql += """
        GROUP BY Proveedor, Proveedor_Nombre
        ORDER BY Deuda DESC
    """

    return run_query(sql, tuple(params))

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

# =====================================================================================
# TABLA RESUMEN POR SOCIEDAD Y BUCKET (DEBAJO DEL TÍTULO)
# =====================================================================================
st.subheader("Exposición por Sociedad y Bucket (MM)")

if resumen.empty:
    st.write("Sin datos para los filtros seleccionados.")
else:
    # Buckets en columnas
    buckets = ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]

    tabla_soc = (
        resumen.groupby("Sociedad")[buckets]
        .sum(numeric_only=True)
        .reset_index()
    )

    # Convertir a valores absolutos en millones (sin decimales)
    for b in buckets:
        tabla_soc[b] = (tabla_soc[b].abs() / 1_000_000).round(0).astype(int)

    # Columna Total en millones también
    tabla_soc["Total"] = tabla_soc[buckets].sum(axis=1).astype(int)

        # Agregar fila de Subtotales
    subtotal_row = pd.DataFrame({
        "Sociedad": ["SUBTOTAL"],
        **{b: [tabla_soc[b].sum()] for b in buckets},
        "Total": [tabla_soc["Total"].sum()]
    })

    tabla_soc_sub = pd.concat([tabla_soc, subtotal_row], ignore_index=True)

    # Estilo: fila SUBTOTAL en negrita
    def bold_subtotal(row):
        return ['font-weight: bold' if row['Sociedad'] == 'SUBTOTAL' else '' 
                for _ in row]

    tabla_style = tabla_soc_sub.style.apply(bold_subtotal, axis=1)

    # Mostrar tabla estilizada
    st.write(tabla_style)



st.divider()


# =====================================================================================
# GRÁFICOS + FILTRO POR BUCKET
# =====================================================================================
st.subheader("Distribución y Análisis por Bucket")

bucket_options = ["(Todos)", "A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]
bucket_sel = st.selectbox("Filtro por bucket", bucket_options, index=0)

# Bar Chart — Distribución por bucket (afectado por el filtro)
if not resumen.empty:
    buckets = ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]
    df_b = pd.DataFrame({
        "Bucket": buckets,
        "Importe": [abs(resumen[b].sum()) for b in buckets]
    })

    # aplicar el mismo filtro del dropdown al gráfico
    if bucket_sel != "(Todos)":
        df_b = df_b[df_b["Bucket"] == bucket_sel]

    fig = px.bar(
        df_b,
        x="Bucket",
        y="Importe",
        text="Importe",
        title="Distribución por bucket"
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor="white",
        paper_bgcolor="white"
    )
    st.plotly_chart(fig, use_container_width=True)

# =====================================================================================
# TREEMAP ABAJO
# =====================================================================================
if not resumen.empty:
    st.subheader("Top 20 Proveedores – Exposición Total")

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
        title=""
    )
    fig3.update_layout(template="plotly_white", paper_bgcolor="white")
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

    # mismo filtro del dropdown aplicado al detalle
    if bucket_sel == "(Todos)":
        det_filtrado = det
    else:
        det_filtrado = det[det["bucket"] == bucket_sel]

    st.dataframe(det_filtrado, use_container_width=True, height=440)

    total_regs = len(det_filtrado)
    if bucket_sel == "(Todos)":
        st.write(f"Cantidad de comprobantes mostrados: {total_regs}")
    else:
        st.write(f"Cantidad de comprobantes mostrados (filtro: {bucket_sel}): {total_regs}")

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
