FROM python:3.9-slim

# Define o diretório de trabalho
WORKDIR /app

# Instala a biblioteca diretamente (mais garantido para o seu teste agora)
RUN pip install --no-cache-dir psycopg2-binary

# Copia os arquivos do seu projeto para o container
COPY . .

# Comando para rodar o script
CMD ["python", "ingestao.py"]