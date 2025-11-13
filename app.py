# =====================================================================================
# GRÁFICOS + FILTRO POR BUCKET
# =====================================================================================
st.subheader("Distribución y Análisis por Bucket")

# Dropdown arriba del gráfico
bucket_options = ["(Todos)", "A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]
bucket_sel = st.selectbox("Filtro por bucket", bucket_options, index=0)

if not resumen.empty:
    # usamos valores absolutos para el gráfico
    buckets = ["A Vencer","0-15","16-60","61-90","91-120","+120","Sin Vto"]
    buck_vals = {b: abs(int0(resumen[b].sum())) for b in buckets}

    df_b = pd.DataFrame({
        "Bucket": buckets,
        "Importe": [buck_vals[b] for b in buckets]
    })

    col1, col2 = st.columns([2,1])

    # Bar chart: Distribución por bucket (solo visual)
    with col1:
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

    # Treemap
    with col2:
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

    # Filtrar según dropdown
    if bucket_sel == "(Todos)":
        det_filtrado = det
    else:
        det_filtrado = det[det["bucket"] == bucket_sel]

    st.dataframe(det_filtrado, use_container_width=True, height=420)

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
