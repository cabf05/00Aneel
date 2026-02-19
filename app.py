import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(page_title="ANEEL Solar Database", layout="wide")

st.title("☀️ Base Completa de Usinas Solares - ANEEL")

RESOURCE_ID = "11ec447d-698d-4ab8-977f-b424d5deee6a"
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

# -------------------------------
# Baixar todos os dados (paginação)
# -------------------------------

@st.cache_data(show_spinner=True)
def carregar_base_completa():
    limit = 1000
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
        time.sleep(0.2)

    df = pd.DataFrame(all_records)
    return df


if st.button("📥 Carregar Base Completa da ANEEL"):

    df = carregar_base_completa()

    st.success(f"{len(df)} registros carregados da ANEEL")

    # Padronização
    if "Potencia Outorgada (kW)" in df.columns:
        df["Potencia MW"] = pd.to_numeric(
            df["Potencia Outorgada (kW)"], errors="coerce"
        ) / 1000

    # Filtro Solar + Operação
    if "Fonte" in df.columns and "Situacao" in df.columns:
        df = df[
            df["Fonte"].str.contains("Solar", case=False, na=False) &
            df["Situacao"].str.contains("Operação", case=False, na=False)
        ]

    st.subheader("🔎 Filtros e Busca")

    col1, col2, col3 = st.columns(3)

    with col1:
        busca = st.text_input("Busca geral (empresa, usina, CNPJ...)")

    with col2:
        estados = st.multiselect(
            "Estado (UF)",
            sorted(df["UF"].dropna().unique())
            if "UF" in df.columns else []
        )

    with col3:
        pot_range = st.slider(
            "Faixa de Potência (MW)",
            0.0,
            float(df["Potencia MW"].max()) if "Potencia MW" in df.columns else 1000.0,
            (1.0, 10.0)
        )

    # Aplicar filtros
    df_filtrado = df.copy()

    if busca:
        df_filtrado = df_filtrado[
            df_filtrado.astype(str)
            .apply(lambda row: row.str.contains(busca, case=False).any(), axis=1)
        ]

    if estados:
        df_filtrado = df_filtrado[df_filtrado["UF"].isin(estados)]

    if "Potencia MW" in df_filtrado.columns:
        df_filtrado = df_filtrado[
            (df_filtrado["Potencia MW"] >= pot_range[0]) &
            (df_filtrado["Potencia MW"] <= pot_range[1])
        ]

    st.subheader("📋 Tabela Completa")

    st.dataframe(
        df_filtrado,
        use_container_width=True,
        height=600
    )

    st.write(f"Total exibido: {len(df_filtrado)} usinas")

    # Download
    csv = df_filtrado.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Baixar CSV filtrado",
        csv,
        "usinas_solares_filtradas.csv",
        "text/csv"
    )
