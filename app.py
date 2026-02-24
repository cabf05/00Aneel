import streamlit as st
import pandas as pd
import requests
import time
import pydeck as pdk
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

st.set_page_config(layout="wide")

st.title("⚡ Brazil Energy Intelligence Dashboard - Usinas & GD")

# =====================================================
# CONFIGURAÇÕES DAS APIs (ANEEL)
# =====================================================
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

RES_USINAS = "11ec447d-698d-4ab8-977f-b424d5deee6a"
RES_GD_INFO = "b1bd71e7-d0ad-4214-9053-cbd58e9564a7"
RES_GD_FOTO = "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"

# =====================================================
# FUNÇÃO GENÉRICA PARA EXTRAÇÃO (COM RETRIES E TIMEOUT MAIOR)
# =====================================================
def fetch_aneel_data(resource_id, limit_per_page=1000, max_records=20000):
    """
    Busca dados na API da ANEEL com proteção contra quedas e timeouts.
    """
    offset = 0
    all_records = []
    
    # Configurando um sistema de tentativas (Retries)
    # Se a API falhar, ele tentará até 3 vezes novamente com um intervalo entre as tentativas
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1, # Espera 1s, depois 2s, depois 4s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Criamos um placeholder no Streamlit para mostrar o progresso ao usuário
    progresso = st.empty()
    
    while offset < max_records:
        progresso.text(f"Baixando dados... {offset} registros carregados de {resource_id[-5:]}")
        params = {"resource_id": resource_id, "limit": limit_per_page, "offset": offset}
        
        try:
            # Aumentamos o timeout de 15 para 60 segundos
            response = session.get(BASE_URL, params=params, timeout=60)
            data = response.json()
            
            if not data.get("success", False):
                break
                
            records = data["result"]["records"]
            if not records:
                break
                
            all_records.extend(records)
            offset += limit_per_page
            time.sleep(0.1) # Respiro para não derrubar a API da ANEEL
            
        except Exception as e:
            st.warning(f"A API da ANEEL está instável. Paramos no registro {offset}. Erro: {e}")
            break # Retorna o que já conseguiu baixar até dar o erro fatal
            
    progresso.empty() # Limpa a mensagem de progresso ao terminar
    return pd.DataFrame(all_records)

# =====================================================
# CARREGAMENTO E UNIFICAÇÃO DOS DADOS
# =====================================================
@st.cache_data(show_spinner=True)
def carregar_dados_unificados():
    
    # 1. Carregar Usinas Centralizadas
    df_usinas = fetch_aneel_data(RES_USINAS, max_records=20000)
    
    # 2. Carregar Geração Distribuída (GD)
    df_gd = fetch_aneel_data(RES_GD_INFO, max_records=20000)
    
    # 3. Carregar Dados Técnicos GD (Fotovoltaica)
    df_gd_tech = fetch_aneel_data(RES_GD_FOTO, max_records=20000)

    # -------------------------------------------------
    # TRATAMENTOS: USINAS
    # -------------------------------------------------
    if not df_usinas.empty:
        df_usinas["Potencia MW"] = pd.to_numeric(
            df_usinas.get("MdaPotenciaOutorgadaKw", "0").astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce"
        ) / 1000
        
        df_usinas["Lat"] = pd.to_numeric(df_usinas.get("NumCoordNEmpreendimento", "").astype(str).str.replace(",", "."), errors="coerce")
        df_usinas["Lon"] = pd.to_numeric(df_usinas.get("NumCoordEEmpreendimento", "").astype(str).str.replace(",", "."), errors="coerce")
        df_usinas = df_usinas[df_usinas.get("DscFaseUsina") == "Operação"]
        
        # Padronizando colunas
        df_usinas = df_usinas.rename(columns={
            "CodCEG": "Codigo",
            "NomEmpreendimento": "Nome",
            "SigUFPrincipal": "UF",
            "DscOrigemCombustivel": "Fonte"
        })
        df_usinas["Categoria"] = "Usina (Geração Centralizada)"

    # -------------------------------------------------
    # TRATAMENTOS: GERAÇÃO DISTRIBUÍDA (MERGE)
    # -------------------------------------------------
    if not df_gd.empty:
        # Merge GD Geral com GD Técnico (Left Join para manter todas as GDs)
        if not df_gd_tech.empty:
            df_gd = df_gd.merge(
                df_gd_tech[['CodGeracaoDistribuida', 'NomFabricanteModulo', 'NomFabricanteInversor']], 
                left_on='CodEmpreendimento', 
                right_on='CodGeracaoDistribuida', 
                how='left'
            )

        df_gd["Potencia MW"] = pd.to_numeric(
            df_gd.get("MdaPotenciaInstaladaKW", "0").astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce"
        ) / 1000
        
        df_gd["Lat"] = pd.to_numeric(df_gd.get("NumCoordNEmpreendimento", "").astype(str).str.replace(",", "."), errors="coerce")
        df_gd["Lon"] = pd.to_numeric(df_gd.get("NumCoordEEmpreendimento", "").astype(str).str.replace(",", "."), errors="coerce")
        
        # Padronizando colunas para bater com Usinas
        df_gd = df_gd.rename(columns={
            "CodEmpreendimento": "Codigo",
            "NomTitularEmpreendimento": "Nome",
            "SigUF": "UF",
            "DscFonteGeracao": "Fonte"
        })
        df_gd["Categoria"] = "Geração Distribuída"

    # -------------------------------------------------
    # CONCATENAR TUDO NO DATAFRAME MESTRE
    # -------------------------------------------------
    cols_padrao = ["Codigo", "Nome", "Categoria", "UF", "Fonte", "Potencia MW", "Lat", "Lon"]
    
    # Garantir que as colunas técnicas existam mesmo se o merge falhar
    if "NomFabricanteModulo" not in df_gd.columns: df_gd["NomFabricanteModulo"] = "-"
    if "NomFabricanteInversor" not in df_gd.columns: df_gd["NomFabricanteInversor"] = "-"
        
    cols_gd = cols_padrao + ["NomFabricanteModulo", "NomFabricanteInversor"]

    df_final = pd.concat([
        df_usinas[cols_padrao] if not df_usinas.empty else pd.DataFrame(columns=cols_padrao),
        df_gd[cols_gd] if not df_gd.empty else pd.DataFrame(columns=cols_gd)
    ], ignore_index=True)

    return df_final.dropna(subset=["Lat", "Lon"]).reset_index(drop=True)

df = carregar_dados_unificados()

# =====================================================
# SIDEBAR FILTROS UNIFICADOS
# =====================================================
st.sidebar.header("Filtros")

categorias = st.sidebar.multiselect("Categoria", sorted(df["Categoria"].dropna().unique()), default=sorted(df["Categoria"].dropna().unique()))
ufs = st.sidebar.multiselect("Estado (UF)", sorted(df["UF"].dropna().unique()))
fontes = st.sidebar.multiselect("Fonte de Energia", sorted(df["Fonte"].dropna().unique()))

pot_min, pot_max = st.sidebar.slider(
    "Capacidade (MW)",
    0.0, float(df["Potencia MW"].max()), 
    (0.0, float(df["Potencia MW"].max()))
)

# Aplicando Filtros
df_filtrado = df.copy()

if categorias: df_filtrado = df_filtrado[df_filtrado["Categoria"].isin(categorias)]
if ufs: df_filtrado = df_filtrado[df_filtrado["UF"].isin(ufs)]
if fontes: df_filtrado = df_filtrado[df_filtrado["Fonte"].isin(fontes)]
df_filtrado = df_filtrado[(df_filtrado["Potencia MW"] >= pot_min) & (df_filtrado["Potencia MW"] <= pot_max)]

# =====================================================
# MÉTRICAS SUPERIORES
# =====================================================
col1, col2, col3 = st.columns(3)
col1.metric("Total de Instalações", f"{len(df_filtrado):,}")
col2.metric("Capacidade Total (MW)", f"{df_filtrado['Potencia MW'].sum():,.2f}")
col3.metric("Estados Atendidos", df_filtrado["UF"].nunique())

st.markdown("---")

# =====================================================
# MAPA CORRIGIDO (TOOLTIPS E RAIO)
# =====================================================
st.subheader("Visão Geoespacial")

map_data = df_filtrado.copy()

if not map_data.empty:
    center_lat = map_data["Lat"].mean()
    center_lon = map_data["Lon"].mean()
else:
    center_lat, center_lon = -14.2350, -51.9253

zoom_level = 6 if len(ufs) == 1 else 4

# Correção 1: Criar a coluna de raio fisicamente no DataFrame
map_data["map_radius"] = map_data["Potencia MW"].apply(lambda x: max(x * 500, 2000)) # Mínimo de 2km para GDs aparecerem

# Correção 2: Tooltip dinâmico baseado na camada renderizada
if len(map_data) > 3000:
    # Camada de Cluster (Agrupada)
    layer = pdk.Layer(
        "HexagonLayer",
        data=map_data,
        get_position='[Lon, Lat]',
        radius=30000,
        elevation_scale=50,
        pickable=True,
        extruded=True,
    )
    # Para HexagonLayer, usamos a contagem do cluster
    tooltip_html = {"html": "<b>Agrupamento</b><br/>Quantidade de Instalações na área: <b>{elevationValue}</b>"}
else:
    # Camada de Pontos (Individual)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_data,
        get_position='[Lon, Lat]',
        get_radius="map_radius", # Puxando a coluna criada
        get_fill_color=[0, 110, 255, 180],
        pickable=True,
    )
    # Para ScatterplotLayer, usamos as colunas do DF padronizado
    tooltip_html = {
        "html": "<b>{Nome}</b><br/>"
                "Tipo: {Categoria}<br/>"
                "Capacidade: {Potencia MW} MW<br/>"
                "Estado: {UF}<br/>"
                "Fonte: {Fonte}"
    }

view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom_level)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style=pdk.map_styles.LIGHT,
    tooltip=tooltip_html
)

st.pydeck_chart(deck)
st.markdown("---")

# =====================================================
# TABELA COMPLETA (AgGrid) UNIFICADA
# =====================================================
st.subheader("Base de Dados Completa")

# Removemos colunas auxiliares do mapa antes de exibir
df_exibicao = df_filtrado.drop(columns=["map_radius"], errors="ignore").sort_values("Potencia MW", ascending=False)

gb = GridOptionsBuilder.from_dataframe(df_exibicao)
gb.configure_default_column(filter=True, sortable=True, resizable=True, floatingFilter=True)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
gb.configure_selection("single", use_checkbox=True)

grid_options = gb.build()

AgGrid(
    df_exibicao,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    theme="streamlit",
    height=500
)

# =====================================================
# DOWNLOAD
# =====================================================
csv = df_exibicao.to_csv(index=False).encode("utf-8")
st.download_button("Baixar Dados Filtrados (CSV)", csv, "dados_energia_brasil.csv", "text/csv")
