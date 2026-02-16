import requests
import pandas as pd
from sqlalchemy import create_engine
import schedule
import time
import sys

# Configurações do seu banco Docker
# O host 'postgres_db' deve ser o nome do serviço no docker-compose.yml
DB_URL = "postgresql://admin:senha_eduzz@postgres_db:5432/meu_data_lake"

# Criando o engine com pool_pre_ping para evitar conexões mortas
engine = create_engine(DB_URL, pool_pre_ping=True)

def coletar_precos():
    # sys.stdout.flush() garante que o print apareça no log do Docker na hora
    print(f"Iniciando coleta em {pd.Timestamp.now()}...", flush=True)
    
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana,cardano,dogecoin",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status() # Garante que erro de API caia no except
        dados = response.json()
        
        # Transforma o JSON em DataFrame
        df = pd.DataFrame(dados).T.reset_index()
        df.columns = ['moeda', 'preco_usd', 'volume_24h', 'mudanca_24h']
        df['timestamp'] = pd.Timestamp.now()

        # Salva no PostgreSQL
        # O método to_sql já gerencia o commit automaticamente
        df.to_sql('monitoramento_cripto', engine, if_exists='append', index=False)
        
        print(f">>> SUCESSO: {len(df)} linhas inseridas na tabela monitoramento_cripto.", flush=True)

    except Exception as e:
        print(f"!!! ERRO NA COLETA OU BANCO: {e}", flush=True)

# Agenda a tarefa para rodar a cada 1 minuto
schedule.every(1).minutes.do(coletar_precos)

if __name__ == "__main__":
    print("Automação ligada. Monitorando logs...", flush=True)
    
    # Roda a primeira vez imediatamente
    coletar_precos() 
    
    while True:
        schedule.run_pending()
        time.sleep(1)