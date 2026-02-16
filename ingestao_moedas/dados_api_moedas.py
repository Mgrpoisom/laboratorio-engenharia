import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import schedule
import time

# --- CONFIGURAÇÃO DE AMBIENTE ---
# Garante a leitura do .env mesmo dentro do Docker
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
caminho_env = os.path.join(diretorio_atual, '.env')
load_dotenv(caminho_env, override=True)

# 1. Captura das variáveis com valores padrão de segurança
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', '5432') # Default para 5432 se vier None
db = os.getenv('DB_NAME')

# 2. Monta a URL de conexão
DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

# Diagnóstico inicial no log
print(f"--- Diagnóstico de Inicialização ---")
print(f"Conectando em: {host}:{port}")
print(f"Banco: {db}")
print(f"------------------------------------")

# Criando o engine
engine = create_engine(DB_URL, pool_pre_ping=True)

def coletar_precos():
    print(f"\nIniciando coleta em {pd.Timestamp.now()}...", flush=True)
    
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana,cardano,dogecoin",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        dados = response.json()
        
        # Tratamento dos dados
        df = pd.DataFrame(dados).T.reset_index()
        df.columns = ['moeda', 'preco_usd', 'volume_24h', 'mudanca_24h']
        df['timestamp'] = pd.Timestamp.now()

        # Inserção no Banco
        df.to_sql('monitoramento_cripto', engine, if_exists='append', index=False)
        
        print(f">>> SUCESSO: {len(df)} linhas inseridas no Postgres.", flush=True)

    except Exception as e:
        print(f"!!! ERRO NA COLETA OU BANCO: {e}", flush=True)

# Agendamento
schedule.every(1).minutes.do(coletar_precos)

if __name__ == "__main__":
    print("Automação ligada. Monitorando logs...", flush=True)
    
    # Executa a primeira vez imediatamente
    coletar_precos() 
    
    while True:
        schedule.run_pending()
        time.sleep(1)