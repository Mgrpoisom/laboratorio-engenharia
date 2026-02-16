import requests
import pandas as pd
from sqlalchemy import create_engine

# 1. Configurações (Sua Chave e Banco Docker)
API_KEY = '75a5b3d4b0b673583d593c38705d8156'
# O host 'localhost' funciona porque você está rodando o script fora do container
DB_URL = "postgresql://admin:senha_eduzz@localhost:5432/meu_data_lake"

def rodar_ingestao_voos():
    # Endereço da API para voos ativos
    url = f"http://api.aviationstack.com/v1/flights?access_key={API_KEY}&limit=100"
    
    try:
        print("Iniciando captura de voos em tempo real...")
        response = requests.get(url)
        response.raise_for_status() # Garante que erros de conexão sejam avisados
        
        dados_brutos = response.json()
        lista_voos = dados_brutos.get('data', [])

        if not lista_voos:
            print("Nenhum dado de voo encontrado no momento.")
            return

        # 2. Transformação (Achatando o JSON complexo)
        # O json_normalize separa sub-itens como 'airline' e 'arrival' em colunas próprias
        df = pd.json_normalize(lista_voos)
        
        # Adiciona carimbo de data/hora para auditoria
        df['ingestao_at'] = pd.Timestamp.now()

        # 3. Carga no PostgreSQL
        print(f"Carregando {len(df)} registros no banco 'meu_data_lake'...")
        engine = create_engine(DB_URL)
        
        # 'append' adiciona os novos dados sem apagar os antigos
        df.to_sql('monitoramento_voos', engine, if_exists='append', index=False)
        
        print("Sucesso! Verifique a tabela 'monitoramento_voos' no DBeaver.")

    except Exception as e:
        print(f"Falha na pipeline: {e}")

if __name__ == "__main__":
    rodar_ingestao_voos()