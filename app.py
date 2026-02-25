import streamlit as st
import pandas as pd
import requests
import json

# Configuração inicial
st.set_page_config(layout="wide", page_title="Download ANEEL")

# Constantes
BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
ID_GD_INFO = "b1bd71e7-d0ad-4214-9053-cbd58e9564a7"
ID_GD_TECH = "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"

@st.cache_data(show_spinner=False)
def buscar_dados(resource_id, filtros=None):
    all_records = []
    offset = 0
    limit = 10000
    session = requests.Session()
    
    status_text = st.empty()
    
    while True:
        params = {"resource_id": resource_id, "limit": limit, "offset": offset}
        if filtros:
            params["filters"] = json.dumps(filtros)
            
        try:
            r = session.get(BASE_URL, params=params, timeout=60)
            data = r.json()
            if not data.get("success"):
                break
            
            records = data["result"]["records"]
            if not records:
                break
                
            df_temp = pd.DataFrame(records)
            # Seleção rigorosa de colunas para não estourar a memória do Streamlit Cloud
            colunas = [
                'SigUF', 'NomTitularEmpreendimento', 'DscFonteGeracao', 
                'MdaPotenciaInstaladaKW', 'CodEmpreendimento',
                'NomFabricanteModulo', 'NomFabricanteInversor',
                'CodGeracaoDistribuida'
            ]
            col_presentes = [c for c in colunas if c in df_temp.columns]
            all_records.append(df_temp[col_presentes])
            
            offset += limit
            status_text.text(f"Carregados {offset} registros...")
            
            if len(records) < limit:
                break
        except Exception as e:
            st.error(f"Erro na conexão: {e}")
            break
            
    status_text.empty()
    if all_records:
        return pd.concat(all_records, ignore_index=True)
    return pd.DataFrame()

# --- Interface ---
st.title("Extração de Dados ANEEL (GD + Equipamentos)")

ufs = st.sidebar.multiselect(
    "Escolha os Estados", 
    ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"],
    default=["SP"]
)

if st.sidebar.button("Processar e Gerar Download"):
    if not ufs:
        st.warning("Selecione uma UF.")
    else:
        # 1. Dados de Equipamentos (Nacional)
        st.info("Buscando Catálogo de Equipamentos... (Aguarde)")
        df_tech = buscar_dados(ID_GD_TECH)
        
        # 2. Dados de GD (Filtrado por UF)
        lista_gd = []
        for uf in ufs:
            st.info(f"Buscando usinas de {uf}...")
            df_uf = buscar_dados(ID_GD_INFO, {"SigUF": uf})
            lista_gd.append(df_uf)
            
        df_base_gd = pd.concat(lista_gd, ignore_index=True) if lista_gd else pd.DataFrame()
        
        # 3. Cruzamento e Botão
        if not df_base_gd.empty and not df_tech.empty:
            st.success("Cruzando dados...")
            
            # Limpa duplicatas técnicas antes do merge
            df_tech = df_tech.drop_duplicates(subset=['CodGeracaoDistribuida'])
            
            df_final = df_base_gd.merge(
                df_tech[['CodGeracaoDistribuida', 'NomFabricanteModulo', 'NomFabricanteInversor']],
                left_on='CodEmpreendimento',
                right_on='CodGeracaoDistribuida',
                how='left'
            )
            
            st.write(f"Total consolidado: {len(df_final)} linhas.")
            st.dataframe(df_final.head(50))
            
            csv = df_final.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Baixar Arquivo CSV",
                data=csv,
                file_name="dados_aneel_consolidado.csv",
                mime="text/csv"
            )
        else:
            st.error("Falha ao obter dados. Tente selecionar menos estados.")
