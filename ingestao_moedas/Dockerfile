# 1. Base Python leve
FROM python:3.9-slim

# 2. Diretório de trabalho no container
WORKDIR /app

# 3. Instalação das dependências necessárias para a CoinGecko e Postgres
RUN pip install --no-cache-dir requests pandas sqlalchemy psycopg2-binary schedule

# 4. Copia apenas o script desta pasta
COPY dados_api_moedas.py .

# 5. Execução do script
CMD ["python", "dados_api_moedas.py"]