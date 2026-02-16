FROM python:3.11-slim

WORKDIR /app

# O Docker agora encontra o arquivo porque o context no yaml é a própria pasta
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia o código (dados_api_moedas.py, .env, etc)
COPY . .

CMD ["python", "dados_api_moedas.py"]