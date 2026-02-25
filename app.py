import streamlit as st
import pandas as pd
import requests
import json
import time

st.set_page_config(layout="wide")

# IDs das tabelas da ANEEL
RECURSOS = {
    "usinas": "11ec447d-698d-4ab8-977f-b424d5deee6a",
    "gd_info": "b1bd71e7-d0ad-4214-9053-cbd58e9564a7",
    "gd_tech": "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"
}

BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

# Função otimizada para baixar dados sem estourar a memória
@st.cache_data(show_spinner=True)
def baixar_dados_aneel(resource_id, filtros=None):
    all_records = []
    offset = 0
    limit = 10000 # Blocos grandes para ser rápido
    
    # Criamos uma sessão para manter a conexão ativa
    session = requests.Session()
    
    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset
        }
        if filtros:
            params["filters"] = json.dumps(filtros)
            
        try:
            r = session.get(BASE_URL, params=params, timeout=30)
            data = r.json()
            if not data["success"]: break
            
            records = data["result"]["records"]
            if not records: break
            
            # Aqui está o segredo: selecionamos apenas o necessário para economizar RAM
            df_temp = pd.DataFrame(records)
            
            # Mantemos apenas colunas que realmente usamos no Dashboard
            colunas_uteis = [
                'SigUF', 'NomTitularEmpreendimento', 'DscFonteGeracao', 
                'MdaPotenciaInstaladaKW', 'NumCoordNEmpreendimento', 
                'NumCoordEEmpreendimento', 'CodEmpreendimento',
                'NomFabricanteModulo', 'NomFabricanteInversor',
                'CodGeracaoDistribuida'
            ]
            colunas_presentes = [c for c in colunas_uteis if c in df_temp.columns]
            all_records.append(df_temp[colunas_presentes])
            
            offset += limit
            if len(records) < limit: break
        except:
            break
            
    return pd.concat(all_records, ignore_index=True) if all_records else pd.DataFrame()

st.title("Baixando Base Completa ANEEL")

# Interface lateral
ufs = st.sidebar.multiselect("Selecione os Estados para baixar", ["SP", "RJ", "MG", "RS", "PR", "SC", "BA"])

if st.sidebar.button("Baixar Dados Agora"):
    if not ufs:
        st.error("Selecione pelo menos um estado.")
    else:
        # 1. Baixa Info de GD (Por UF para não dar timeout)
        lista_gd = []
        prog = st.progress(0)
        for i, uf in enumerate(ufs):
            st.write(f"Baixando dados de {uf}...")
            df_uf = baixar_dados_aneel(RECURSOS["gd_info"], {"SigUF": uf})
            lista_gd.append(df_uf)
            prog.progress((i + 1) / len(ufs))
        
        df_final_gd = pd.concat(lista_gd, ignore_index=True)
        
        # 2. Baixa Dados Técnicos (Equipamentos) - Essa tabela é global
        st.write("Baixando Catálogo de Equipamentos (Módulos/Inversores)...")
        df_tech = baixar_dados_aneel(RECURSOS["gd_tech"])
        
        # 3. Faz o Cruzamento (Merge)
        st.write("Cruzando dados técnicos com a base de usinas...")
        if not df_final_gd.empty and not df_tech.empty:
            # O merge vincula o fabricante ao ID da usina
            df_consolidado = df_final_gd.merge(
                df_tech[['CodGeracaoDistribuida', 'NomFabricanteModulo', 'NomFabricanteInversor']],
                left_on='CodEmpreendimento',
                right_on='CodGeracaoDistribuida',
                how='left'
            )
            
            st.success(f"Sucesso! {len(df_consolidado)} registros carregados.")
            
            # 4. Mostra a Tabela com os dados de Fabricantes
            st.subheader("Visualização dos Dados (Com Equipamentos)")
            st.dataframe(df_consolidado[[
                'SigUF', 'NomTitularEmpreendimento', 'MdaPotenciaInstaladaKW', 
                'NomFabricanteModulo', 'NomFabricanteInversor'
            ]].head(100))
            
            # Botão de Download para você salvar no seu PC
            csv = df_consolidado.to_csv(index=False).encode('utf-8')
            st.download_button("Clique aqui para baixar o CSV completo", csv, "base_aneel_completa.csv", "text/csv")            records = data["result"]["records"]
            if not records:
                break
                
            todos_registros.extend(records)
            total_api = data["result"]["total"]
            
            print(f"Progresso: {len(todos_registros)} / {total_api}", end="\r")
            
            offset += limit
            if offset >= total_api:
                break
                
        except Exception as e:
            print(f"\nErro ao baixar bloco: {e}. Tentando salvar o que já temos...")
            break
            
    return pd.DataFrame(todos_registros)

# --- EXECUÇÃO DO PROCESSO ---

# 1. Baixar as 3 tabelas
df_usinas = extrair_tudo(RECURSOS["usinas"], "Usinas Centralizadas")
df_gd = extrair_tudo(RECURSOS["gd_info"], "Geração Distribuída (Geral)")
df_tech = extrair_tudo(RECURSOS["gd_equipamentos"], "Geração Distribuída (Técnica)")

# 2. Tratamento de Dados (Garantir que os equipamentos apareçam)
print("\nProcessando e Unificando dados...")

# Cruzamento da GD com Equipamentos
if not df_gd.empty and not df_tech.empty:
    df_gd = df_gd.merge(
        df_tech[["CodGeracaoDistribuida", "NomFabricanteModulo", "NomFabricanteInversor"]],
        left_on="CodEmpreendimento",
        right_on="CodGeracaoDistribuida",
        how="left"
    )

# 3. Limpeza de colunas e nomes (Padronização)
# (Aqui você mantém as renomeações que já tínhamos no seu código do Streamlit)

# 4. Salvar para o Dashboard
# Usamos Parquet porque o arquivo de 2 milhões de linhas ficaria gigante em CSV
df_gd.to_parquet("base_gd_consolidada.parquet", index=False)
df_usinas.to_parquet("base_usinas_consolidada.parquet", index=False)

print("\n✅ Sucesso! Arquivos 'base_gd_consolidada.parquet' e 'base_usinas_consolidada.parquet' gerados.")
