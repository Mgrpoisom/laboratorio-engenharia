
import time
# ... outros imports

print("Iniciando a automação...") # Isso TEM que aparecer no log
while True:
    try:
        # Seu código de coleta aqui
        print("Coleta realizada com sucesso!")
        time.sleep(60)
    except Exception as e:
        print(f"Erro: {e}")
        time.sleep(10)