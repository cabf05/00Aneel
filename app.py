import streamlit as st
import pandas as pd
import requests
import time
import pydeck as pdk
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

st.set_page_config(layout="wide")

st.title("Dashboard Dados – ANEEL")

RESOURCE_ID = "11ec447d-698d-4ab8-977f-b424d5deee6a"
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"


# ==============================
# CARREGAMENTO OTIMIZADO
# ==============================

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

    # ------------------------
    # Tratamentos importantes
    # ------------------------

    # Converter potência corretamente
    df["MdaPotenciaOutorgadaKw"] = (
        df["MdaPotenciaOutorgadaKw"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["Potencia MW"] = pd.to_numeric(
        df["MdaPotenciaOutorgadaKw"], errors="coerce"
    ) / 1000

    # Converter coordenadas
    df["Lat"] = pd.to_numeric(
        df["NumCoordNEmpreendimento"]
        .astype(str)
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )

    df["Lon"] = pd.to_numeric(
        df["NumCoordEEmpreendimento"]
        .astype(str)
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )

    df = df[df["DscFaseUsina"] == "Operação"]

    return df.reset_index(drop=True)


df = carregar_base()

# ==============================
# SIDEBAR FILTROS
# ==============================

st.sidebar.header("Filtros")

ufs = st.sidebar.multiselect(
    "Estado",
    sorted(df["SigUFPrincipal"].dropna().unique())
)

fontes = st.sidebar.multiselect(
    "Fonte",
    sorted(df["DscOrigemCombustivel"].dropna().unique())
)

pot_min, pot_max = st.sidebar.slider(
    "Faixa Potência (MW)",
    0.0,
    float(df["Potencia MW"].max()),
    (0.0, float(df["Potencia MW"].max()))
)

df_filtrado = df.copy()

if ufs:
    df_filtrado = df_filtrado[df_filtrado["SigUFPrincipal"].isin(ufs)]

if fontes:
    df_filtrado = df_filtrado[df_filtrado["DscOrigemCombustivel"].isin(fontes)]

df_filtrado = df_filtrado[
    (df_filtrado["Potencia MW"] >= pot_min) &
    (df_filtrado["Potencia MW"] <= pot_max)
]

# ==============================
# MÉTRICAS
# ==============================

col1, col2, col3 = st.columns(3)

col1.metric("Total Usinas", f"{len(df_filtrado):,}")
col2.metric("Potência Total (MW)", f"{df_filtrado['Potencia MW'].sum():,.2f}")
col3.metric("Estados", df_filtrado["SigUFPrincipal"].nunique())


# ==============================
# MAPA
# ==============================

st.subheader("📍 Mapa Interativo")

map_data = df_filtrado.dropna(subset=["Lat", "Lon"])

layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_data,
    get_position='[Lon, Lat]',
    get_radius=20000,
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=-14.2350,
    longitude=-51.9253,
    zoom=4
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={
        "html": "<b>{NomEmpreendimento}</b><br/>"
                "Potência: {Potencia MW} MW<br/>"
                "UF: {SigUFPrincipal}<br/>"
                "Fonte: {DscOrigemCombustivel}"
    }
)

st.pydeck_chart(deck)


# ==============================
# TABELA RESUMIDA
# ==============================

st.subheader("📊 Tabela Resumida")

df_resumo = df_filtrado[
    [
        "NomEmpreendimento",
        "Potencia MW",
        "SigUFPrincipal",
        "DscMuninicpios",
        "DscOrigemCombustivel",
        "DscPropriRegimePariticipacao"
    ]
]

st.dataframe(
    df_resumo.sort_values("Potencia MW", ascending=False),
    use_container_width=True,
    height=400
)


# ==============================
# TABELA COMPLETA (AgGrid)
# ==============================

st.subheader("📋 Base Completa")

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
    fit_columns_on_grid_load=False,
    theme="streamlit",
    height=600
)
