FROM python:3.11-slim

WORKDIR /app

# Copia explicitamente o arquivo para dentro do container
COPY requirements.txt /app/requirements.txt

# Instala as dependências verificando se o arquivo existe
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos
COPY . /app/

CMD ["python", "dados_api_moedas.py"]

