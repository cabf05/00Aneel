import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(page_title="ANEEL Solar Database", layout="wide")

st.title("☀️ Base Completa de Usinas Solares - ANEEL")

RESOURCE_ID = "11ec447d-698d-4ab8-977f-b424d5deee6a"
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

# -------------------------------
# FUNÇÃO PARA BAIXAR TODOS DADOS
# -------------------------------

@st.cache_data(show_spinner=True)
def baixar_tudo():
    limit = 1000
    offset = 0
    all_records = []

    while True:
        url = BASE_URL
        params = {
            "resource_id": RESOURCE_ID,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params)
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


if st.button("📥 Baixar Base Completa ANEEL"):

    df = baixar_tudo()

    st.success(f"{len(df)} registros carregados")

    # Padronização básica
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

    st.subheader("📊 Filtros Comerciais")

    estados = st.multiselect(
        "Filtrar por Estado",
        options=sorted(df["UF"].dropna().unique())
        if "UF" in df.columns else []
    )

    pot_min = st.number_input("Potência mínima (MW)", 0.0, 10000.0, 1.0)
    pot_max = st.number_input("Potência máxima (MW)", 0.0, 10000.0, 10.0)

    if estados:
        df = df[df["UF"].isin(estados)]

    if "Potencia MW" in df.columns:
        df = df[
            (df["Potencia MW"] >= pot_min) &
            (df["Potencia MW"] <= pot_max)
        ]

    st.subheader("📋 Tabela de Usinas")
    st.dataframe(df, use_container_width=True)

    if "Nome da Outorgada" in df.columns:
        ranking = (
            df.groupby("Nome da Outorgada")["Potencia MW"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        st.subheader("🏆 Ranking por Empresa (MW Total)")
        st.dataframe(ranking, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Baixar CSV",
        csv,
        "usinas_solares_aneel.csv",
        "text/csv"
    )
