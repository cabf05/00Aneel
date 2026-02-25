import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from urllib3.util.retry import Retry

st.set_page_config(layout="wide", page_title="Brazil Energy Intelligence")
st.title("Brazil Energy Intelligence Dashboard - Usinas & GD")

BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
RES_USINAS  = "11ec447d-698d-4ab8-977f-b424d5deee6a"
RES_GD_INFO = "b1bd71e7-d0ad-4214-9053-cbd58e9564a7"
RES_GD_FOTO = "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"
UF_COL_USINAS  = "SigUFPrincipal"
UF_COL_GD_INFO = "SigUF"

ESTADOS_BR = sorted([
    "AC","AL","AM","AP","BA","CE","DF","ES","GO",
    "MA","MG","MS","MT","PA","PB","PE","PI","PR",
    "RJ","RN","RO","RR","RS","SC","SE","SP","TO"
])

def make_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_all_pages(resource_id, filters=None, limit_per_page=1000):
    """
    Busca TODOS os registros usando o campo 'total' da API como criterio de parada.
    Sem limite artificial - garante que todos os dados sejam retornados.
    """
    session = make_session()
    offset = 0
    total = None
    all_records = []

    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit_per_page,
            "offset": offset,
        }
        if filters:
            params["filters"] = json.dumps(filters)

        try:
            response = session.get(BASE_URL, params=params, timeout=60)
            data = response.json()

            if not data.get("success", False):
                break

            result = data["result"]

            if total is None:
                total = result.get("total", 0)
                print("Recurso " + resource_id[-8:] + " | filtro=" + str(filters) + " | total=" + str(total))

            records = result.get("records", [])
            if not records:
                break

            all_records.extend(records)
            offset += limit_per_page

            if offset >= total:
                break

            time.sleep(0.1)

        except Exception as e:
            print("Erro offset=" + str(offset) + ": " + str(e))
            break

    return pd.DataFrame(all_records)

def fetch_uf(resource_id, uf_column, uf):
    return fetch_all_pages(resource_id, filters={uf_column: uf})

@st.cache_data(show_spinner=False)
def carregar_raw(ufs_tuple):
    ufs = list(ufs_tuple)
    todas = set(ufs) >= set(ESTADOS_BR)

    if todas:
        with ThreadPoolExecutor(max_workers=3) as ex:
            f_us   = ex.submit(fetch_all_pages, RES_USINAS)
            f_gd   = ex.submit(fetch_all_pages, RES_GD_INFO)
            f_foto = ex.submit(fetch_all_pages, RES_GD_FOTO)
        return f_us.result(), f_gd.result(), f_foto.result()

    # Modo por UF: busca usinas e gd_info filtrados por estado
    # GD Foto nao tem coluna UF - busca tudo e o merge filtra naturalmente
    max_workers = min(len(ufs) * 2 + 1, 8)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_usinas  = {ex.submit(fetch_uf, RES_USINAS,  UF_COL_USINAS,  uf): uf for uf in ufs}
        fut_gd_info = {ex.submit(fetch_uf, RES_GD_INFO, UF_COL_GD_INFO, uf): uf for uf in ufs}
        # GD Foto sempre completo para o merge nao perder registros
        fut_foto = ex.submit(fetch_all_pages, RES_GD_FOTO)

        parts_usinas = []
        for fut in as_completed(fut_usinas):
            try:
                parts_usinas.append(fut.result())
            except Exception as e:
                print("Erro usinas: " + str(e))

        parts_gd = []
        for fut in as_completed(fut_gd_info):
            try:
                parts_gd.append(fut.result())
            except Exception as e:
                print("Erro gd_info: " + str(e))

        df_foto = fut_foto.result()

    df_usinas = pd.concat(parts_usinas, ignore_index=True) if parts_usinas else pd.DataFrame()
    df_gd     = pd.concat(parts_gd,     ignore_index=True) if parts_gd     else pd.DataFrame()

    return df_usinas, df_gd, df_foto

@st.cache_data(show_spinner=False)
def carregar_dados_unificados(ufs_tuple):
    df_usinas, df_gd, df_gd_tech = carregar_raw(ufs_tuple)

    if not df_usinas.empty:
        df_usinas["Potencia MW"] = pd.to_numeric(
            df_usinas.get("MdaPotenciaOutorgadaKw", pd.Series(dtype=str))
                .astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce"
        ) / 1000
        df_usinas["Lat"] = pd.to_numeric(
            df_usinas.get("NumCoordNEmpreendimento", pd.Series(dtype=str))
                .astype(str).str.replace(",", "."), errors="coerce"
        )
        df_usinas["Lon"] = pd.to_numeric(
            df_usinas.get("NumCoordEEmpreendimento", pd.Series(dtype=str))
                .astype(str).str.replace(",", "."), errors="coerce"
        )
        if "DscFaseUsina" in df_usinas.columns:
            df_usinas = df_usinas[df_usinas["DscFaseUsina"] == "Opera\u00e7\u00e3o"]
        df_usinas = df_usinas.rename(columns={
            "CodCEG":               "Codigo",
            "NomEmpreendimento":    "Nome",
            "SigUFPrincipal":       "UF",
            "DscOrigemCombustivel": "Fonte",
        })
        df_usinas["Categoria"] = "Usina (Geracao Centralizada)"

    if not df_gd.empty:
        # Merge com GD Foto para trazer dados tecnicos dos equipamentos
        if not df_gd_tech.empty:
            # Garante que a chave de join esta como string nos dois lados
            df_gd["CodEmpreendimento"] = df_gd["CodEmpreendimento"].astype(str).str.strip()
            df_gd_tech["CodGeracaoDistribuida"] = df_gd_tech["CodGeracaoDistribuida"].astype(str).str.strip()

            cols_foto = ["CodGeracaoDistribuida", "NomFabricanteModulo", "NomFabricanteInversor"]
            # Filtra apenas colunas que existem na tabela de foto
            cols_foto = [c for c in cols_foto if c in df_gd_tech.columns]

            if len(cols_foto) > 1:
                df_gd = df_gd.merge(
                    df_gd_tech[cols_foto],
                    left_on="CodEmpreendimento",
                    right_on="CodGeracaoDistribuida",
                    how="left"
                )

        df_gd["Potencia MW"] = pd.to_numeric(
            df_gd.get("MdaPotenciaInstaladaKW", pd.Series(dtype=str))
                .astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce"
        ) / 1000
        df_gd["Lat"] = pd.to_numeric(
            df_gd.get("NumCoordNEmpreendimento", pd.Series(dtype=str))
                .astype(str).str.replace(",", "."), errors="coerce"
        )
        df_gd["Lon"] = pd.to_numeric(
            df_gd.get("NumCoordEEmpreendimento", pd.Series(dtype=str))
                .astype(str).str.replace(",", "."), errors="coerce"
        )
        df_gd = df_gd.rename(columns={
            "CodEmpreendimento":        "Codigo",
            "NomTitularEmpreendimento": "Nome",
            "SigUF":                    "UF",
            "DscFonteGeracao":          "Fonte",
        })
        df_gd["Categoria"] = "Geracao Distribuida"

    cols_padrao = ["Codigo", "Nome", "Categoria", "UF", "Fonte", "Potencia MW", "Lat", "Lon"]

    for col in ["NomFabricanteModulo", "NomFabricanteInversor"]:
        if col not in df_gd.columns:
            df_gd[col] = "-"

    cols_gd = cols_padrao + ["NomFabricanteModulo", "NomFabricanteInversor"]

    df_final = pd.concat([
        df_usinas[cols_padrao] if not df_usinas.empty else pd.DataFrame(columns=cols_padrao),
        df_gd[cols_gd]         if not df_gd.empty    else pd.DataFrame(columns=cols_gd),
    ], ignore_index=True)

    return df_final.dropna(subset=["Lat", "Lon"]).reset_index(drop=True)

# =====================================================
# UI - SELECAO DE ESTADOS
# =====================================================

st.sidebar.header("Selecao de Estados")

selecionar_todos = st.sidebar.checkbox("Selecionar todos os estados", value=False)

if selecionar_todos:
    ufs_escolhidas = ESTADOS_BR
    st.sidebar.caption("Todos os " + str(len(ESTADOS_BR)) + " estados selecionados.")
else:
    ufs_escolhidas = st.sidebar.multiselect(
        "Estados (UF)",
        options=ESTADOS_BR,
        default=["SP", "RJ"],
        help="Selecione um ou mais estados."
    )

if not ufs_escolhidas:
    st.warning("Selecione ao menos um estado na barra lateral.")
    st.stop()

carregar = st.sidebar.button("Carregar / Atualizar Dados", type="primary", use_container_width=True)

chave_cache = tuple(sorted(ufs_escolhidas))

if "chave_atual" not in st.session_state or st.session_state["chave_atual"] != chave_cache:
    if not carregar:
        if len(ufs_escolhidas) <= 5:
            estados_str = ", ".join(ufs_escolhidas)
        else:
            estados_str = str(len(ufs_escolhidas)) + " estados"
        st.info("Estados selecionados: " + estados_str + ". Clique em Carregar / Atualizar Dados.")
        st.stop()

st.session_state["chave_atual"] = chave_cache

if len(ufs_escolhidas) <= 5:
    modo = ", ".join(ufs_escolhidas)
else:
    modo = str(len(ufs_escolhidas)) + " estados"

# Aviso para selecao grande
if selecionar_todos:
    st.warning("Modo completo selecionado. A carga inicial pode levar varios minutos. O cache evita repeticao nas proximas visitas.")

with st.spinner("Buscando dados para " + modo + "... Aguarde."):
    df = carregar_dados_unificados(chave_cache)

if df.empty:
    st.error("Nenhum dado retornado. Tente novamente ou selecione outros estados.")
    st.stop()

st.sidebar.success(str(len(df)) + " instalacoes carregadas.")
st.sidebar.markdown("---")

# =====================================================
# FILTROS SECUNDARIOS
# =====================================================

st.sidebar.header("Filtros")

categorias = st.sidebar.multiselect(
    "Categoria",
    sorted(df["Categoria"].dropna().unique()),
    default=sorted(df["Categoria"].dropna().unique())
)
fontes = st.sidebar.multiselect("Fonte de Energia", sorted(df["Fonte"].dropna().unique()))

pot_max_val = float(df["Potencia MW"].max()) if df["Potencia MW"].max() > 0 else 1.0
pot_min, pot_max = st.sidebar.slider("Capacidade (MW)", 0.0, pot_max_val, (0.0, pot_max_val))

df_filtrado = df.copy()
if categorias:
    df_filtrado = df_filtrado[df_filtrado["Categoria"].isin(categorias)]
if fontes:
    df_filtrado = df_filtrado[df_filtrado["Fonte"].isin(fontes)]
df_filtrado = df_filtrado[
    (df_filtrado["Potencia MW"] >= pot_min) &
    (df_filtrado["Potencia MW"] <= pot_max)
]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Instalacoes",  str(len(df_filtrado)))
col2.metric("Capacidade Total (MW)", str(round(df_filtrado["Potencia MW"].sum(), 2)))
col3.metric("Estados",               str(df_filtrado["UF"].nunique()))
col4.metric("Fontes distintas",      str(df_filtrado["Fonte"].nunique()))

st.markdown("---")

# =====================================================
# MAPA
# =====================================================

st.subheader("Visao Geoespacial")

map_data = df_filtrado.copy()
center_lat = map_data["Lat"].mean() if not map_data.empty else -14.2350
center_lon = map_data["Lon"].mean() if not map_data.empty else -51.9253

if len(ufs_escolhidas) == 1:
    zoom_level = 6
elif len(ufs_escolhidas) <= 5:
    zoom_level = 5
else:
    zoom_level = 4

map_data["map_radius"] = map_data["Potencia MW"].apply(lambda x: max(x * 500, 2000))

if len(map_data) > 3000:
    layer = pdk.Layer(
        "HexagonLayer", data=map_data, get_position="[Lon, Lat]",
        radius=30000, elevation_scale=50, pickable=True, extruded=True,
    )
    tooltip_html = {"html": "<b>Agrupamento</b><br/>Instalacoes na area: <b>{elevationValue}</b>"}
else:
    layer = pdk.Layer(
        "ScatterplotLayer", data=map_data, get_position="[Lon, Lat]",
        get_radius="map_radius", get_fill_color=[0, 110, 255, 180], pickable=True,
    )
    tooltip_html = {
        "html": "<b>{Nome}</b><br/>Tipo: {Categoria}<br/>"
                "Capacidade: {Potencia MW} MW<br/>Estado: {UF}<br/>Fonte: {Fonte}"
    }

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom_level),
    map_style=pdk.map_styles.LIGHT,
    tooltip=tooltip_html
))

st.markdown("---")

# =====================================================
# TABELA E DOWNLOAD
# =====================================================

st.subheader("Base de Dados Completa")

df_exibicao = (
    df_filtrado
    .drop(columns=["map_radius"], errors="ignore")
    .sort_values("Potencia MW", ascending=False)
)

gb = GridOptionsBuilder.from_dataframe(df_exibicao)
gb.configure_default_column(filter=True, sortable=True, resizable=True, floatingFilter=True)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
gb.configure_selection("single", use_checkbox=True)

AgGrid(
    df_exibicao,
    gridOptions=gb.build(),
    update_mode=GridUpdateMode.NO_UPDATE,
    theme="streamlit",
    height=500
)

csv = df_exibicao.to_csv(index=False).encode("utf-8")
st.download_button(
    "Baixar Dados Filtrados (CSV)",
    csv,
    "dados_energia_brasil.csv",
    "text/csv"
)
