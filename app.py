import streamlit as st
import requests
import pandas as pd
import time
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

st.set_page_config(page_title="ANEEL Solar Database", layout="wide")

st.title("☀️ Base Completa de Usinas Solares - ANEEL")

RESOURCE_ID = "11ec447d-698d-4ab8-977f-b424d5deee6a"
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"


# =====================================================
# CARREGAMENTO COM CACHE
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

    # -------------------------
    # Padronizações importantes
    # -------------------------

    if "Potencia Outorgada (kW)" in df.columns:
        df["Potencia MW"] = (
            pd.to_numeric(df["Potencia Outorgada (kW)"], errors="coerce") / 1000
        )

    # Filtrar Solar + Operação já no carregamento
    if "Fonte" in df.columns and "Situacao" in df.columns:
        df = df[
            df["Fonte"].str.contains("Solar", case=False, na=False) &
            df["Situacao"].str.contains("Operação", case=False, na=False)
        ]

    # Resetar índice para performance
    df = df.reset_index(drop=True)

    return df


# =====================================================
# CARREGAR BASE UMA VEZ
# =====================================================

df = carregar_base()

st.success(f"{len(df):,} usinas solares em operação carregadas")


# =====================================================
# BUSCA GLOBAL OTIMIZADA
# =====================================================

st.subheader("🔎 Busca Global")

busca = st.text_input("Digite qualquer termo (empresa, CNPJ, município...)")

df_filtrado = df

if busca:
    mask = df.astype(str).apply(
        lambda col: col.str.contains(busca, case=False, na=False)
    )
    df_filtrado = df[mask.any(axis=1)]


st.write(f"Total após filtro: {len(df_filtrado):,}")


# =====================================================
# AGGRID PROFISSIONAL OTIMIZADO
# =====================================================

st.subheader("📊 Base Completa")

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

gb.configure_selection("multiple", use_checkbox=True)

grid_options = gb.build()

AgGrid(
    df_filtrado,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    fit_columns_on_grid_load=False,
    theme="streamlit",
    enable_enterprise_modules=False,
    height=650,
    reload_data=False
)


# =====================================================
# DOWNLOAD
# =====================================================

csv = df_filtrado.to_csv(index=False).encode("utf-8")

st.download_button(
    "📥 Baixar CSV filtrado",
    csv,
    "usinas_solares_filtradas.csv",
    "text/csv"
)
