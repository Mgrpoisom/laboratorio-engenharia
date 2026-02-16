import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# 1. Configuração de Caminhos e Variáveis
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
caminho_env = os.path.join(diretorio_atual, '.env')
load_dotenv(caminho_env, override=True)

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT', '5432')
db_name = os.getenv('DB_NAME')

# 2. Configuração da URL e Engine
DB_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DB_URL, pool_pre_ping=True)

# 3. Função de Conexão Resiliente (O Coração da Solução)
def conectar_com_retry(engine, tentativas=5, intervalo=5):
    for i in range(tentativas):
        try:
            print(f"--- Tentativa de conexão {i+1}/{tentativas} ---")
            with engine.connect() as conn:
                print("SUCESSO: Conexão estabelecida com o Postgres!")
                return True
        except OperationalError as e:
            print(f"Banco ainda não disponível (Postgres iniciando...). Aguardando {intervalo}s...")
            time.sleep(intervalo)
    
    print("ERRO CRÍTICO: Não foi possível conectar ao banco após várias tentativas.")
    return False

# 4. Execução Principal
if __name__ == "__main__":
    if conectar_com_retry(engine):
        print(" Iniciando a coleta de dados da API...")
        # AQUI VOCÊ CHAMA SUA FUNÇÃO DE COLETA, ex:
        # dados = coletar_dados_api()
        # salvar_no_banco(dados, engine)
    else:
        print("Saindo do script por falta de conexão.")