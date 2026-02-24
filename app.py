import streamlit as st
import pandas as pd
import requests
import time
import pydeck as pdk
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

st.set_page_config(layout="wide")

st.title("⚡ Brazil Energy Intelligence Dashboard")

RESOURCE_ID = "11ec447d-698d-4ab8-977f-b424d5deee6a"
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"


# =====================================================
# CARREGAMENTO
# =====================================================

@st.cache_data(show_spinner=True)
def carregar_base():
    limit = 5000
    offset = 0
    all_records = []

    while True:
        params = {
            "resource_id": RESOURCE_ID,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if not data["success"]:
            break

        records = data["result"]["records"]
        if not records:
            break

        all_records.extend(records)
        offset += limit
        time.sleep(0.05)

    df = pd.DataFrame(all_records)

    # Tratamentos
    df["MdaPotenciaOutorgadaKw"] = (
        df["MdaPotenciaOutorgadaKw"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["Potencia MW"] = pd.to_numeric(
        df["MdaPotenciaOutorgadaKw"], errors="coerce"
    ) / 1000

    df["Lat"] = pd.to_numeric(
        df["NumCoordNEmpreendimento"].astype(str).str.replace(",", "."),
        errors="coerce"
    )

    df["Lon"] = pd.to_numeric(
        df["NumCoordEEmpreendimento"].astype(str).str.replace(",", "."),
        errors="coerce"
    )

    df = df[df["DscFaseUsina"] == "Operação"]

    return df.reset_index(drop=True)


df = carregar_base()

# =====================================================
# SIDEBAR
# =====================================================

st.sidebar.header("Filters")

ufs = st.sidebar.multiselect(
    "State (UF)",
    sorted(df["SigUFPrincipal"].dropna().unique())
)

fontes = st.sidebar.multiselect(
    "Energy Source",
    sorted(df["DscOrigemCombustivel"].dropna().unique())
)

df_filtrado = df.copy()

if ufs:
    df_filtrado = df_filtrado[df_filtrado["SigUFPrincipal"].isin(ufs)]

if fontes:
    df_filtrado = df_filtrado[df_filtrado["DscOrigemCombustivel"].isin(fontes)]

# =====================================================
# MÉTRICAS
# =====================================================

col1, col2, col3 = st.columns(3)

col1.metric("Plants", f"{len(df_filtrado):,}")
col2.metric("Total Capacity (MW)", f"{df_filtrado['Potencia MW'].sum():,.2f}")
col3.metric("States", df_filtrado["SigUFPrincipal"].nunique())

st.markdown("---")

# =====================================================
# ZOOM INTELIGENTE
# =====================================================

if not df_filtrado.empty:
    center_lat = df_filtrado["Lat"].mean()
    center_lon = df_filtrado["Lon"].mean()
else:
    center_lat = -14.2350
    center_lon = -51.9253

zoom_level = 6 if len(ufs) == 1 else 4


# =====================================================
# CLUSTER AUTOMÁTICO
# =====================================================

map_data = df_filtrado.dropna(subset=["Lat", "Lon"])

layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_data,
    get_position='[Lon, Lat]',
    get_radius="Potencia MW * 200",
    get_fill_color=[0, 110, 255, 180],
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom_level
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style=pdk.map_styles.LIGHT,
    tooltip={
        "html": "<b>{NomEmpreendimento}</b><br/>"
                "Capacity: {Potencia MW} MW<br/>"
                "State: {SigUFPrincipal}<br/>"
                "Source: {DscOrigemCombustivel}"
    }
)

st.subheader("Geospatial View")
event = st.pydeck_chart(deck)

# =====================================================
# CLIQUE NO PONTO → FILTRAR
# =====================================================

if event and "object" in event and event["object"]:
    nome_usina = event["object"]["NomEmpreendimento"]
    df_filtrado = df_filtrado[
        df_filtrado["NomEmpreendimento"] == nome_usina
    ]
    st.info(f"Filtered by selected plant: {nome_usina}")

st.markdown("---")

# =====================================================
# TABELA RESUMIDA
# =====================================================

st.subheader("Filtered Plants")

df_resumo = df_filtrado[
    [
        "NomEmpreendimento",
        "Potencia MW",
        "SigUFPrincipal",
        "DscMuninicpios",
        "DscOrigemCombustivel",
        "DscPropriRegimePariticipacao"
    ]
].sort_values("Potencia MW", ascending=False)

st.dataframe(
    df_resumo,
    use_container_width=True,
    height=350
)

st.markdown("---")

# =====================================================
# TABELA COMPLETA PROFISSIONAL
# =====================================================

st.subheader("Full Dataset")

gb = GridOptionsBuilder.from_dataframe(df_filtrado)

gb.configure_default_column(
    filter=True,
    sortable=True,
    resizable=True,
    floatingFilter=True
)

gb.configure_pagination(
    paginationAutoPageSize=False,
    paginationPageSize=100
)

grid_options = gb.build()

AgGrid(
    df_filtrado,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    theme="streamlit",
    height=600
)
