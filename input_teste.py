
import psycopg2

def inserir_manual():
    try:
        # Conexão com o banco Docker
        conn = psycopg2.connect(
            host="localhost",
            database="meu_data_lake",
            user="admin",
            password="senha_eduzz"
        )
        cursor = conn.cursor()

        print("--- Ingestão Manual de Vendas ---")
        
        # Coleta de dados via Input
        produto = input("Qual o nome do produto? ")
        valor = float(input("Qual o valor do produto? (Ex: 299.90): "))

        # Comando SQL
        sql = "INSERT INTO vendas_teste (produto, valor) VALUES (%s, %s)"
        cursor.execute(sql, (produto, valor))

        conn.commit()
        print(f"\nSucesso: '{produto}' inserido no banco!")

    except Exception as e:
        print(f"\nErro: {e}")
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    inserir_manual()