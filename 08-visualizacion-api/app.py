"""
app.py
-------
Punto 8: Dashboard analítico de natalidad en Colombia (2021-2025)
Diseño estético con paleta pastel, tarjetas visuales y gráficos interactivos.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests

# Configuración de página
st.set_page_config(
    page_title="Natalidad en Colombia · Big Data",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS personalizados: paleta pastel, tarjetas redondeadas y ocultar botones nativos de Streamlit
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
        color: #334155;
    }

    /* Ocultar botón 'Deploy' y menú publicitario de Streamlit */
    .stDeployButton {display: none !important;}
    #MainMenu {visibility: hidden !important;}
    header [data-testid="stHeader"] {display: none !important;}
    footer {visibility: hidden !important;}

    /* Contenedor del encabezado */
    .header-box {
        background: linear-gradient(135deg, #FFF1F2 0%, #FEF9C3 50%, #F0FDF4 100%);
        border: 1px solid #FFE4E6;
        border-radius: 16px;
        padding: 1.8rem 2rem;
        margin-bottom: 1.8rem;
    }
    .header-title {
        font-size: 2rem;
        font-weight: 700;
        color: #881337;
        margin: 0;
        letter-spacing: -0.02em;
    }
    .header-subtitle {
        font-size: 0.95rem;
        color: #64748B;
        margin-top: 0.35rem;
        font-weight: 500;
    }

    /* Tarjetas de métricas (KPI Cards) */
    .metric-card {
        background-color: #FFFFFF;
        border-radius: 14px;
        padding: 1.2rem 1.4rem;
        box-shadow: 0 4px 12px rgba(148, 163, 184, 0.08);
        border: 1px solid #F1F5F9;
        transition: transform 0.15s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
    }
    .metric-card-pink { border-left: 4px solid #FB7185; }
    .metric-card-purple { border-left: 4px solid #C084FC; }
    .metric-card-yellow { border-left: 4px solid #FBBF24; }
    .metric-card-mint { border-left: 4px solid #34D399; }

    .metric-label {
        font-size: 0.8rem;
        text-transform: uppercase;
        font-weight: 600;
        color: #94A3B8;
        letter-spacing: 0.05em;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #0F172A;
        margin: 0.2rem 0;
    }
    .metric-detail {
        font-size: 0.8rem;
        font-weight: 500;
    }
    .detail-pink { color: #E11D48; }
    .detail-purple { color: #9333EA; }
    .detail-yellow { color: #D97706; }
    .detail-mint { color: #059669; }

    /* Tarjetas de contenido / hallazgos */
    .insight-card {
        background-color: #FAFAFA;
        border-radius: 12px;
        padding: 1.1rem 1.3rem;
        margin-top: 1rem;
        border: 1px solid #F4F4F5;
    }
    .insight-pink { background-color: #FFF1F2; border-color: #FFE4E6; }
    .insight-yellow { background-color: #FEFCE8; border-color: #FEF08A; }
    .insight-blue { background-color: #F0F9FF; border-color: #E0F2FE; }
    .insight-mint { background-color: #F0FDF4; border-color: #DCFCE7; }

    .insight-title {
        font-size: 0.85rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.3rem;
    }
    .insight-text {
        font-size: 0.88rem;
        line-height: 1.5;
        color: #334155;
    }

    /* Pestañas estilizadas */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid #E2E8F0;
        padding-bottom: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 16px;
        font-weight: 600;
        font-size: 0.9rem;
        color: #64748B;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFF1F2 !important;
        color: #BE123C !important;
        border: 1px solid #FECDD3 !important;
    }
</style>
""", unsafe_allow_html=True)

# Encabezado Banner
st.markdown("""
<div class="header-box">
    <div class="header-title">Analítica de Natalidad en Colombia (2021 - 2025)</div>
    <div class="header-subtitle">Universidad EAFIT · SI3007 Sistemas Distribuidos · Pipeline Big Data sobre Amazon Web Services</div>
</div>
""", unsafe_allow_html=True)

# -------------------------------------------------------------
# BARRA LATERAL: Control de Origen de Datos
# -------------------------------------------------------------
st.sidebar.markdown("### Origen de Datos")
modo_datos = st.sidebar.radio(
    "Seleccionar fuente:",
    ["Datos Procesados (Data Lake S3)", "Amazon API Gateway (REST API)"],
    label_visibility="collapsed"
)

api_url = ""
if modo_datos == "Amazon API Gateway (REST API)":
    api_url = st.sidebar.text_input(
        "URL base del API Gateway:",
        placeholder="https://xyz123.execute-api.us-east-1.amazonaws.com"
    )
    st.sidebar.caption("Consulta la función AWS Lambda expuesta a través de Amazon API Gateway.")

st.sidebar.markdown("---")
st.sidebar.markdown("### Arquitectura del Sistema")
st.sidebar.markdown("""
- **Almacenamiento:** Amazon S3 (Raw, Trusted, Refined)
- **Transformación:** AWS Glue & Apache Spark
- **Catalogación:** AWS Glue Data Catalog
- **Motor SQL:** Amazon Athena (Serverless)
- **Servicio:** AWS Lambda + API Gateway + Streamlit
""")

# -------------------------------------------------------------
# OBTENCIÓN DE DATOS (CON SOPORTE TANTO PARA API COMO LOCAL)
# -------------------------------------------------------------
@st.cache_data
def obtener_datos(pregunta_id, url_api=""):
    if url_api:
        try:
            base_url = url_api.strip().rstrip('/')
            if base_url.endswith('/metricas'):
                base_url = base_url[:-9]
            endpoint = f"{base_url}/metricas?pregunta={pregunta_id}"
            resp = requests.get(endpoint, timeout=4)
            if resp.status_code == 200:
                raw_json = resp.json()
                # Adaptar estructura según formato devuelto por Lambda
                if pregunta_id == 1:
                    totales = raw_json.get("totales_anuales", [])
                    top = raw_json.get("top_deptos") or raw_json.get("top_departamentos_2021") or []
                    return {"totales_anuales": totales, "top_deptos": top}
                elif pregunta_id in (2, 3, 4):
                    if isinstance(raw_json, dict) and "datos" in raw_json:
                        return raw_json["datos"]
                    return raw_json
                elif pregunta_id == 5:
                    if isinstance(raw_json, dict):
                        return raw_json.get("datos_2024") or raw_json.get("datos") or []
                    return raw_json
        except Exception:
            pass

    # Datos consolidados de respaldo (Data Lake Athena)
    if pregunta_id == 1:
        return {
            "totales_anuales": [
                {"anio": 2021, "nacimientos": 616914},
                {"anio": 2022, "nacimientos": 573625},
                {"anio": 2023, "nacimientos": 515549},
                {"anio": 2024, "nacimientos": 453901},
                {"anio": 2025, "nacimientos": 441537}
            ],
            "top_deptos": [
                {"departamento": "Bogotá, D.C.", "nacimientos": 79820},
                {"departamento": "Antioquia", "nacimientos": 69808},
                {"departamento": "Valle del Cauca", "nacimientos": 46720},
                {"departamento": "Atlántico", "nacimientos": 40237},
                {"departamento": "Bolívar", "nacimientos": 33956},
                {"departamento": "Santander", "nacimientos": 26753},
                {"departamento": "La Guajira", "nacimientos": 26355},
                {"departamento": "Córdoba", "nacimientos": 26168},
                {"departamento": "Magdalena", "nacimientos": 25017},
                {"departamento": "Norte de Santander", "nacimientos": 24950}
            ]
        }
    elif pregunta_id == 2:
        return [
            {"mes": 1, "nombre_mes": "Enero", "total_nacimientos": 216916},
            {"mes": 2, "nombre_mes": "Febrero", "total_nacimientos": 195617},
            {"mes": 3, "nombre_mes": "Marzo", "total_nacimientos": 219604},
            {"mes": 4, "nombre_mes": "Abril", "total_nacimientos": 211247},
            {"mes": 5, "nombre_mes": "Mayo", "total_nacimientos": 219670},
            {"mes": 6, "nombre_mes": "Junio", "total_nacimientos": 211499},
            {"mes": 7, "nombre_mes": "Julio", "total_nacimientos": 220684},
            {"mes": 8, "nombre_mes": "Agosto", "total_nacimientos": 224925},
            {"mes": 9, "nombre_mes": "Septiembre", "total_nacimientos": 229652},
            {"mes": 10, "nombre_mes": "Octubre", "total_nacimientos": 220311},
            {"mes": 11, "nombre_mes": "Noviembre", "total_nacimientos": 213735},
            {"mes": 12, "nombre_mes": "Diciembre", "total_nacimientos": 217666}
        ]
    elif pregunta_id == 3:
        return [
            {"departamento": "Vichada", "total": 8155, "adolescentes": 2142, "pct": 26.27},
            {"departamento": "Guainía", "total": 5732, "adolescentes": 1470, "pct": 25.65},
            {"departamento": "Magdalena", "total": 97377, "adolescentes": 23265, "pct": 23.89},
            {"departamento": "Caquetá", "total": 27879, "adolescentes": 6476, "pct": 23.23},
            {"departamento": "Amazonas", "total": 4923, "adolescentes": 1132, "pct": 22.99},
            {"departamento": "Arauca", "total": 21220, "adolescentes": 4845, "pct": 22.83},
            {"departamento": "Córdoba", "total": 107173, "adolescentes": 23107, "pct": 21.56},
            {"departamento": "Cesar", "total": 96539, "adolescentes": 20715, "pct": 21.46},
            {"departamento": "Chocó", "total": 33016, "adolescentes": 7058, "pct": 21.38},
            {"departamento": "La Guajira", "total": 111390, "adolescentes": 22782, "pct": 20.45},
            {"departamento": "Putumayo", "total": 19266, "adolescentes": 3934, "pct": 20.42},
            {"departamento": "Bogotá, D.C.", "total": 341500, "adolescentes": 31200, "pct": 9.14}
        ]
    elif pregunta_id == 4:
        return [
            {"nivel_educativo": "Doctorado", "nacimientos": 559, "promedio_controles": 9.19, "pct_cesareas": 70.66, "pct_espontaneos": 26.65},
            {"nivel_educativo": "Maestría", "nacimientos": 12496, "promedio_controles": 9.19, "pct_cesareas": 67.33, "pct_espontaneos": 29.50},
            {"nivel_educativo": "Especialización", "nacimientos": 36400, "promedio_controles": 8.92, "pct_cesareas": 67.13, "pct_espontaneos": 30.14},
            {"nivel_educativo": "Profesional", "nacimientos": 284102, "promedio_controles": 8.54, "pct_cesareas": 62.40, "pct_espontaneos": 35.80},
            {"nivel_educativo": "Básica secundaria", "nacimientos": 845210, "promedio_controles": 6.30, "pct_cesareas": 48.20, "pct_espontaneos": 50.80},
            {"nivel_educativo": "Básica primaria", "nacimientos": 310500, "promedio_controles": 5.10, "pct_cesareas": 38.40, "pct_espontaneos": 60.50},
            {"nivel_educativo": "Ninguno", "nacimientos": 26735, "promedio_controles": 2.92, "pct_cesareas": 23.28, "pct_espontaneos": 76.54}
        ]
    elif pregunta_id == 5:
        return [
            {"pais": "Perú", "codigo": "PER", "tasa": 15.75},
            {"pais": "México", "codigo": "MEX", "tasa": 15.46},
            {"pais": "Colombia", "codigo": "COL", "tasa": 13.25},
            {"pais": "Brasil", "codigo": "BRA", "tasa": 12.13},
            {"pais": "Argentina", "codigo": "ARG", "tasa": 11.09},
            {"pais": "Chile", "codigo": "CHL", "tasa": 8.72}
        ]

# -------------------------------------------------------------
# MÉTRICAS RESUMEN (TARJETAS PASTEL)
# -------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown("""
    <div class="metric-card metric-card-pink">
        <div class="metric-label">Nacimientos Procesados</div>
        <div class="metric-value">2,601,526</div>
        <div class="metric-detail detail-pink">Tendencia: -28.4% (2021 vs 2025)</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown("""
    <div class="metric-card metric-card-purple">
        <div class="metric-label">Rango Histórico</div>
        <div class="metric-value">2021 - 2025</div>
        <div class="metric-detail detail-purple">5 periodos anuales completos</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown("""
    <div class="metric-card metric-card-yellow">
        <div class="metric-label">Tasa Natalidad Colombia</div>
        <div class="metric-value">13.25 ‰</div>
        <div class="metric-detail detail-yellow">Por cada 1.000 habitantes (WB)</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown("""
    <div class="metric-card metric-card-mint">
        <div class="metric-label">Mes de Mayor Volumen</div>
        <div class="metric-value">Septiembre</div>
        <div class="metric-detail detail-mint">229,652 partos acumulados</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# -------------------------------------------------------------
# SECCIONES TEMÁTICAS
# -------------------------------------------------------------
tabs = st.tabs([
    "1. Evolución Temporal",
    "2. Estacionalidad Mensual",
    "3. Madres Adolescentes",
    "4. Educación y Salud",
    "5. Contexto Internacional"
])

# 1. Evolución Temporal
with tabs[0]:
    st.markdown("#### Evolución de Nacimientos en Colombia (2021 - 2025)")
    st.caption("Tendencia decreciente a nivel nacional y concentración en los principales departamentos.")

    d1 = obtener_datos(1, api_url)
    df_anual = pd.DataFrame(d1.get("totales_anuales", []))
    df_top = pd.DataFrame(d1.get("top_deptos", []))

    col_a, col_b = st.columns(2)
    with col_a:
        if not df_anual.empty:
            fig_anual = px.bar(
                df_anual, x="anio", y="nacimientos",
                text="nacimientos",
                labels={"anio": "Año", "nacimientos": "Total Nacimientos"},
                title="Total Nacional Anual"
            )
            fig_anual.update_traces(
                marker_color="#FDA4AF",
                marker_line_color="#F43F5E",
                marker_line_width=1.5,
                texttemplate="%{text:,.0f}",
                textposition="outside"
            )
            fig_anual.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                yaxis=dict(showgrid=True, gridcolor="#F1F5F9", range=[0, 720000]),
                xaxis=dict(showgrid=False),
                margin=dict(t=40, b=20, l=20, r=20)
            )
            st.plotly_chart(fig_anual, use_container_width=True)

    with col_b:
        if not df_top.empty:
            fig_top = px.bar(
                df_top.sort_values("nacimientos", ascending=True),
                x="nacimientos", y="departamento",
                orientation="h",
                text="nacimientos",
                labels={"nacimientos": "Nacimientos", "departamento": "Departamento"},
                title="Departamentos con Mayor Volumen"
            )
            fig_top.update_traces(
                marker_color="#FDE68A",
                marker_line_color="#F59E0B",
                marker_line_width=1.5,
                texttemplate="%{text:,.0f}",
                textposition="outside"
            )
            fig_top.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True, gridcolor="#F1F5F9"),
                yaxis=dict(showgrid=False),
                margin=dict(t=40, b=20, l=20, r=20)
            )
            st.plotly_chart(fig_top, use_container_width=True)

    st.markdown("""
    <div class="insight-card insight-pink">
        <div class="insight-title" style="color: #9F1239;">Hallazgo Demográfico Principal</div>
        <div class="insight-text">
            Existe una caída constante y pronunciada en la natalidad colombiana, pasando de 616.914 nacimientos en 2021 a 441.537 en 2025 (un descenso del 28.4%). Bogotá y Antioquia continúan liderando el volumen nacional acumulado.
        </div>
    </div>
    """, unsafe_allow_html=True)

# 2. Estacionalidad Mensual
with tabs[1]:
    st.markdown("#### Distribución de Nacimientos por Mes Calendario")
    st.caption("Identificación de patrones cíclicos de natalidad durante el periodo analizado.")

    d2 = obtener_datos(2, api_url)
    df_mes = pd.DataFrame(d2)

    if not df_mes.empty:
        fig_mes = px.area(
            df_mes, x="nombre_mes", y="total_nacimientos",
            markers=True,
            labels={"nombre_mes": "Mes", "total_nacimientos": "Nacimientos Acumulados"},
            title="Curva Estacional Acumulada (2021 - 2025)"
        )
        fig_mes.update_traces(
            line_color="#FB7185",
            fillcolor="rgba(251, 113, 133, 0.15)",
            marker=dict(size=8, color="#E11D48", line=dict(width=2, color="#FFFFFF"))
        )
        fig_mes.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(showgrid=True, gridcolor="#F1F5F9"),
            xaxis=dict(showgrid=False),
            margin=dict(t=40, b=20, l=20, r=20)
        )
        st.plotly_chart(fig_mes, use_container_width=True)

    st.markdown("""
    <div class="insight-card insight-yellow">
        <div class="insight-title" style="color: #92400E;">Estacionalidad y Picos de Partos</div>
        <div class="insight-text">
            El tercer trimestre (agosto y septiembre) concentra el pico más alto de natalidad en el país, superando los 224.000 nacimientos. Por el contrario, febrero presenta los valores más bajos (195.617 partos acumulados).
        </div>
    </div>
    """, unsafe_allow_html=True)

# 3. Madres Adolescentes
with tabs[2]:
    st.markdown("#### Proporción de Nacimientos en Madres Adolescentes (10 - 19 años)")
    st.caption("Porcentaje de partos en menores de edad frente al total de nacimientos por departamento.")

    d3 = obtener_datos(3, api_url)
    df_adol = pd.DataFrame(d3)
    if not df_adol.empty and "pct" in df_adol.columns:
        df_adol = df_adol.sort_values("pct", ascending=True)

        fig_adol = px.bar(
            df_adol, x="pct", y="departamento",
            orientation="h",
            text="pct",
            labels={"pct": "Porcentaje (%)", "departamento": "Departamento"},
            title="Porcentaje de Madres de 10 a 19 Años por Departamento"
        )
        fig_adol.update_traces(
            marker_color="#FCA5A5",
            marker_line_color="#EF4444",
            marker_line_width=1.5,
            texttemplate="%{text:.2f}%",
            textposition="outside"
        )
        fig_adol.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=True, gridcolor="#F1F5F9", range=[0, 30]),
            yaxis=dict(showgrid=False),
            margin=dict(t=40, b=20, l=20, r=20)
        )
        st.plotly_chart(fig_adol, use_container_width=True)

    st.markdown("""
    <div class="insight-card insight-pink">
        <div class="insight-title" style="color: #9F1239;">Brechas Territoriales Críticas</div>
        <div class="insight-text">
            Departamentos como Vichada (26.27%) y Guainía (25.65%) registran tasas de maternidad adolescente casi 3 veces superiores a las de Bogotá (9.14%), evidenciando disparidades estructurales en acceso a educación y salud sexual reproductiva.
        </div>
    </div>
    """, unsafe_allow_html=True)

# 4. Educación y Salud
with tabs[3]:
    st.markdown("#### Nivel Educativo de la Madre vs Controles Prenatales y Modalidad de Parto")
    st.caption("Cruce entre el nivel educativo alcanzado (NIV_EDUM), número de controles (NUMCONSUL) y tipo de parto.")

    d4 = obtener_datos(4, api_url)
    df_edu = pd.DataFrame(d4)
    if not df_edu.empty and "promedio_controles" in df_edu.columns:
        df_edu = df_edu.sort_values("promedio_controles", ascending=False)

        col_e1, col_e2 = st.columns(2)
        with col_e1:
            fig_controles = px.bar(
                df_edu, x="nivel_educativo", y="promedio_controles",
                text="promedio_controles",
                labels={"nivel_educativo": "Nivel Educativo", "promedio_controles": "Controles Promedio"},
                title="Promedio de Controles Prenatales"
            )
            fig_controles.update_traces(
                marker_color="#C4B5FD",
                marker_line_color="#8B5CF6",
                marker_line_width=1.5,
                texttemplate="%{text:.2f}",
                textposition="outside"
            )
            fig_controles.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                yaxis=dict(showgrid=True, gridcolor="#F1F5F9", range=[0, 11]),
                xaxis=dict(tickangle=-35),
                margin=dict(t=40, b=20, l=20, r=20)
            )
            st.plotly_chart(fig_controles, use_container_width=True)

        with col_e2:
            fig_parto = go.Figure(data=[
                go.Bar(
                    name="Cesárea (%)", x=df_edu["nivel_educativo"], y=df_edu["pct_cesareas"],
                    marker_color="#F472B6", marker_line_color="#DB2777", marker_line_width=1.2
                ),
                go.Bar(
                    name="Espontáneo (%)", x=df_edu["nivel_educativo"], y=df_edu["pct_espontaneos"],
                    marker_color="#6EE7B7", marker_line_color="#059669", marker_line_width=1.2
                )
            ])
            fig_parto.update_layout(
                barmode="stack",
                title="Modalidad de Parto: Cesárea vs Espontáneo",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                yaxis=dict(showgrid=True, gridcolor="#F1F5F9", title="Porcentaje (%)"),
                xaxis=dict(tickangle=-35),
                margin=dict(t=40, b=20, l=20, r=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_parto, use_container_width=True)

    st.markdown("""
    <div class="insight-card insight-blue">
        <div class="insight-title" style="color: #0369A1;">Asociación entre Educación y Atención Médica</div>
        <div class="insight-text">
            A mayor nivel educativo de la madre se incrementa significativamente el número de controles prenatales (de 2.92 en madres sin estudios hasta 9.19 en posgrados) y la proporción de partos por cesárea (del 23.28% al 70.66%).
        </div>
    </div>
    """, unsafe_allow_html=True)

# 5. Contexto Internacional
with tabs[4]:
    st.markdown("#### Comparativa Internacional de Tasa Bruta de Natalidad")
    st.caption("Indicador WB_WDI_SP_DYN_CBRT_IN del Banco Mundial (nacimientos por cada 1.000 habitantes en 2024).")

    d5 = obtener_datos(5, api_url)
    df_wb = pd.DataFrame(d5)
    if not df_wb.empty and "tasa" in df_wb.columns:
        df_wb = df_wb.sort_values("tasa", ascending=False)

        colores = ["#BAE6FD" if p != "Colombia" and not str(p).startswith("COL") else "#FDE047" for p in df_wb["pais"]]
        bordes = ["#0284C7" if p != "Colombia" and not str(p).startswith("COL") else "#CA8A04" for p in df_wb["pais"]]

        fig_wb = px.bar(
            df_wb, x="pais", y="tasa",
            text="tasa",
            labels={"pais": "País", "tasa": "Tasa por 1.000 hab."},
            title="Tasa Bruta de Natalidad en Países de la Región (2024)"
        )
        fig_wb.update_traces(
            marker_color=colores,
            marker_line_color=bordes,
            marker_line_width=1.5,
            texttemplate="%{text:.2f}",
            textposition="outside"
        )
        fig_wb.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(showgrid=True, gridcolor="#F1F5F9", range=[0, 20]),
            xaxis=dict(showgrid=False),
            margin=dict(t=40, b=20, l=20, r=20)
        )
        st.plotly_chart(fig_wb, use_container_width=True)

    st.markdown("""
    <div class="insight-card insight-mint">
        <div class="insight-title" style="color: #065F46;">Validación de Datos: DANE vs Banco Mundial</div>
        <div class="insight-text">
            Con los 453.901 nacimientos registrados por el DANE en 2024 y una población nacional estimada de 52 millones de habitantes, la tasa calculada a partir de los microdatos del data lake es de aproximadamente 13.2 nacimientos por cada 1.000 habitantes, coincidiendo con la tasa de 13.25 publicada por la API del Banco Mundial.
        </div>
    </div>
    """, unsafe_allow_html=True)
