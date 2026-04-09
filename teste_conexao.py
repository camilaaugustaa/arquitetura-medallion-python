import psycopg2
try:
    conn = psycopg2.connect(
        host="localhost", 
        port=5432, 
        database="postgres", 
        user="postgres", 
        password="password"
    )
    print("Conexão bem-sucedida!")
    conn.close()
except Exception as e:
    print(f"Erro ao conectar: {e}")
