import streamlit as st
import pandas as pd
import requests
import time
import pydeck as pdk
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(layout="wide")
st.title("⚡ Brazil Energy Intelligence Dashboard - Usinas & GD")

# =====================================================
# CONFIGURAÇÕES DAS APIs (ANEEL)
# =====================================================
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

RES_USINAS   = "11ec447d-698d-4ab8-977f-b424d5deee6a"
RES_GD_INFO  = "b1bd71e7-d0ad-4214-9053-cbd58e9564a7"
RES_GD_FOTO  = "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"

# =====================================================
# FUNÇÃO GENÉRICA PARA EXTRAÇÃO (SEM st.empty — thread-safe)
# =====================================================
def fetch_aneel_data(resource_id, limit_per_page=1000, max_records=20000):
    """
    Busca dados na API da ANEEL.
    ⚠️ Sem chamadas ao st.* aqui: esta função roda em threads secundárias
    e o Streamlit não suporta comandos de UI fora da thread principal.
    """
    offset = 0
    all_records = []

    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    while offset < max_records:
        params = {
            "resource_id": resource_id,
            "limit": limit_per_page,
            "offset": offset
        }
        try:
            response = session.get(BASE_URL, params=params, timeout=60)
            data = response.json()

            if not data.get("success", False):
                break

            records = data["result"]["records"]
            if not records:
                break

            all_records.extend(records)
            offset += limit_per_page
            time.sleep(0.1)

        except Exception as e:
            # Retorna o que já foi baixado até o momento
            print(f"[AVISO] Recurso {resource_id[-8:]}: parou no offset {offset}. Erro: {e}")
            break

    return pd.DataFrame(all_records)


# =====================================================
# CARREGAMENTO PARALELO COM CACHE
# =====================================================
@st.cache_data(show_spinner=False)
def carregar_raw_paralelo():
    """
    Dispara os 3 fetches em paralelo usando ThreadPoolExecutor.
    O cache garante que isso só roda UMA VEZ por sessão de deploy.
    """
    tarefas = {
        "usinas":   (RES_USINAS,  20000),
        "gd_info":  (RES_GD_INFO, 20000),
        "gd_foto":  (RES_GD_FOTO, 20000),
    }

    resultados = {}

    # max_workers=3 pois temos exatamente 3 fontes independentes
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(fetch_aneel_data, res_id, 1000, max_rec): nome
            for nome, (res_id, max_rec) in tarefas.items()
        }
        for future in as_completed(futures):
            nome = futures[future]
            try:
                resultados[nome] = future.result()
            except Exception as e:
                st.warning(f"Falha ao carregar '{nome}': {e}")
                resultados[nome] = pd.DataFrame()

    return resultados["usinas"], resultados["gd_info"], resultados["gd_foto"]


@st.cache_data(show_spinner=False)
def carregar_dados_unificados():
    """
    Orquestra o carregamento paralelo e aplica todos os tratamentos.
    Separado do fetch para que o cache de transformação também seja aproveitado.
    """

    # --- Barra de progresso visível para o usuário ---
    with st.spinner("⏳ Carregando bases em paralelo (Usinas + GD Info + GD Foto)..."):
        df_usinas, df_gd, df_gd_tech = carregar_raw_paralelo()

    # -------------------------------------------------
    # TRATAMENTOS: USINAS
    # -------------------------------------------------
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
            df_usinas = df_usinas[df_usinas["DscFaseUsina"] == "Operação"]

        df_usinas = df_usinas.rename(columns={
            "CodCEG":               "Codigo",
            "NomEmpreendimento":    "Nome",
            "SigUFPrincipal":       "UF",
            "DscOrigemCombustivel": "Fonte",
        })
        df_usinas["Categoria"] = "Usina (Geração Centralizada)"

    # -------------------------------------------------
    # TRATAMENTOS: GERAÇÃO DISTRIBUÍDA
    # -------------------------------------------------
    if not df_gd.empty:
        if not df_gd_tech.empty:
            df_gd = df_gd.merge(
                df_gd_tech[["CodGeracaoDistribuida", "NomFabricanteModulo", "NomFabricanteInversor"]],
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
            "CodEmpreendimento":         "Codigo",
            "NomTitularEmpreendimento":  "Nome",
            "SigUF":                     "UF",
            "DscFonteGeracao":           "Fonte",
        })
        df_gd["Categoria"] = "Geração Distribuída"

    # -------------------------------------------------
    # CONCATENAR TUDO NO DATAFRAME MESTRE
    # -------------------------------------------------
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


df = carregar_dados_unificados()

# =====================================================
# SIDEBAR FILTROS
# =====================================================
st.sidebar.header("Filtros")

categorias = st.sidebar.multiselect(
    "Categoria", sorted(df["Categoria"].dropna().unique()),
    default=sorted(df["Categoria"].dropna().unique())
)
ufs   = st.sidebar.multiselect("Estado (UF)", sorted(df["UF"].dropna().unique()))
fontes = st.sidebar.multiselect("Fonte de Energia", sorted(df["Fonte"].dropna().unique()))

pot_min, pot_max = st.sidebar.slider(
    "Capacidade (MW)",
    0.0, float(df["Potencia MW"].max()),
    (0.0, float(df["Potencia MW"].max()))
)

df_filtrado = df.copy()
if categorias: df_filtrado = df_filtrado[df_filtrado["Categoria"].isin(categorias)]
if ufs:        df_filtrado = df_filtrado[df_filtrado["UF"].isin(ufs)]
if fontes:     df_filtrado = df_filtrado[df_filtrado["Fonte"].isin(fontes)]
df_filtrado = df_filtrado[
    (df_filtrado["Potencia MW"] >= pot_min) &
    (df_filtrado["Potencia MW"] <= pot_max)
]

# =====================================================
# MÉTRICAS
# =====================================================
col1, col2, col3 = st.columns(3)
col1.metric("Total de Instalações",  f"{len(df_filtrado):,}")
col2.metric("Capacidade Total (MW)", f"{df_filtrado['Potencia MW'].sum():,.2f}")
col3.metric("Estados Atendidos",     df_filtrado["UF"].nunique())
st.markdown("---")

# =====================================================
# MAPA
# =====================================================
st.subheader("Visão Geoespacial")

map_data = df_filtrado.copy()
center_lat = map_data["Lat"].mean() if not map_data.empty else -14.2350
center_lon = map_data["Lon"].mean() if not map_data.empty else -51.9253
zoom_level = 6 if len(ufs) == 1 else 4

map_data["map_radius"] = map_data["Potencia MW"].apply(lambda x: max(x * 500, 2000))

if len(map_data) > 3000:
    layer = pdk.Layer(
        "HexagonLayer", data=map_data, get_position="[Lon, Lat]",
        radius=30000, elevation_scale=50, pickable=True, extruded=True,
    )
    tooltip_html = {"html": "<b>Agrupamento</b><br/>Instalações na área: <b>{elevationValue}</b>"}
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
# TABELA + DOWNLOAD
# =====================================================
st.subheader("Base de Dados Completa")

df_exibicao = df_filtrado.drop(columns=["map_radius"], errors="ignore").sort_values("Potencia MW", ascending=False)

gb = GridOptionsBuilder.from_dataframe(df_exibicao)
gb.configure_default_column(filter=True, sortable=True, resizable=True, floatingFilter=True)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
gb.configure_selection("single", use_checkbox=True)

AgGrid(df_exibicao, gridOptions=gb.build(),
       update_mode=GridUpdateMode.NO_UPDATE, theme="streamlit", height=500)

csv = df_exibicao.to_csv(index=False).encode("utf-8")
st.download_button("Baixar Dados Filtrados (CSV)", csv, "dados_energia_brasil.csv", "text/csv")
