import requests
import pandas as pd
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurações de Recursos da ANEEL
RECURSOS = {
    "usinas": "11ec447d-698d-4ab8-977f-b424d5deee6a",
    "gd_info": "b1bd71e7-d0ad-4214-9053-cbd58e9564a7",
    "gd_equipamentos": "49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"
}

BASE_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

def extrair_tudo(resource_id, nome_amigavel):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    
    offset = 0
    limit = 10000 # Pedindo blocos grandes para agilizar
    todos_registros = []
    
    print(f"\nIniciando extração de: {nome_amigavel}")
    
    while True:
        try:
            params = {"resource_id": resource_id, "limit": limit, "offset": offset}
            response = session.get(BASE_URL, params=params, timeout=90)
            data = response.json()
            
            if not data["success"]:
                break
                
            records = data["result"]["records"]
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
