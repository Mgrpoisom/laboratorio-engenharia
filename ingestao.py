
import psycopg2

try:
    # 1. Conectar ao banco que está no Docker
    # Como o script roda no seu Windows, o 'host' é localhost
    conn = psycopg2.connect(
        host="banco-dados",
        database="meu_data_lake",
        user="admin",
        password="senha_eduzz"
    )
    cursor = conn.cursor()

    # 2. Criar uma tabela de teste (Engenharia de Dados)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vendas_teste (
            id SERIAL PRIMARY KEY,
            produto VARCHAR(100),
            valor DECIMAL,
            data_venda TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 3. Inserir um dado fictício
    cursor.execute(
        "INSERT INTO vendas_teste (produto, valor) VALUES (%s, %s)",
        ("Assinatura Eduzz Premium", 299.90)
    )

    conn.commit()
    print("Dados inseridos com sucesso no banco Dockerizado!")

except Exception as e:
    print(f" Erro ao conectar: {e}")
finally:
    if 'conn' in locals():
        cursor.close()
        conn.close()
