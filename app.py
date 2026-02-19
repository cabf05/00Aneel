import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="ANEEL Solar Leads", layout="wide")

st.title("☀️ Mapeamento de Parques Solares - ANEEL")

RESOURCE_ID = "11ec447d-698d-4ab8-977f-b424d5deee6a"
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"

st.sidebar.header("Filtros")

estado = st.sidebar.text_input("Estado (UF)", "")
pot_min = st.sidebar.number_input("Potência mínima (MW)", min_value=0.0, value=1.0)
pot_max = st.sidebar.number_input("Potência máxima (MW)", min_value=0.0, value=10.0)
limite = st.sidebar.slider("Quantidade de registros", 100, 5000, 1000)

if st.sidebar.button("Buscar Dados"):

    query = f"""
    SELECT *
    FROM "{RESOURCE_ID}"
    WHERE 
        "Fonte" ILIKE '%Solar%'
        AND "Situacao" ILIKE '%Operação%'
        AND CAST("Potencia Outorgada (kW)" AS numeric) >= {pot_min * 1000}
        AND CAST("Potencia Outorgada (kW)" AS numeric) <= {pot_max * 1000}
    """

    if estado:
        query += f" AND \"UF\" ILIKE '{estado.upper()}'"

    query += f" LIMIT {limite}"

    response = requests.get(BASE_URL, params={"sql": query})
    data = response.json()

    if not data["success"]:
        st.error("Erro ao consultar API.")
    else:
        records = data["result"]["records"]
        df = pd.DataFrame(records)

        if df.empty:
            st.warning("Nenhum resultado encontrado.")
        else:
            # Conversão potência para MW
            df["Potencia MW"] = pd.to_numeric(df["Potencia Outorgada (kW)"], errors="coerce") / 1000

            st.success(f"{len(df)} usinas encontradas")

            st.dataframe(df, use_container_width=True)

            # Ranking por empresa
            if "Nome da Outorgada" in df.columns:
                ranking = (
                    df.groupby("Nome da Outorgada")["Potencia MW"]
                    .sum()
                    .sort_values(ascending=False)
                    .reset_index()
                )

                st.subheader("📊 Ranking por Empresa (MW total)")
                st.dataframe(ranking, use_container_width=True)

            # Download CSV
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Baixar CSV",
                csv,
                "usinas_solares_filtradas.csv",
                "text/csv"
            )
