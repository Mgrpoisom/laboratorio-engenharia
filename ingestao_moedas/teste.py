import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Pega o caminho da pasta onde o script está (ingestao_moedas)
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
caminho_env = os.path.join(diretorio_atual, '.env')

# Força o carregamento do arquivo específico
load_dotenv(caminho_env, override=True)

# Agora pegamos as variáveis
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# LOG DE DIAGNÓSTICO (Para você ver no terminal se carregou)
print(f"--- Diagnóstico de Conexão ---")
print(f"Arquivo .env encontrado: {os.path.exists(caminho_env)}")
print(f"Host carregado: {db_host}")
print(f"Porta carregada: {db_port}")
print(f"------------------------------")

# Se a porta ainda for None, forçamos o padrão para não quebrar o int()
if not db_port:
    db_port = "5432"

DB_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DB_URL, pool_pre_ping=True)