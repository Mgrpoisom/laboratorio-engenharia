FROM python:3.11-slim

WORKDIR /app

# Copia o requirements que está na raiz do projeto
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia o conteúdo da pasta da aplicação para o container
COPY ./ingestao_moedas/ .

CMD ["python", "dados_api_moedas.py"]
