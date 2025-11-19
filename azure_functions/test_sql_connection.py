import pytds
import certifi

# ==== DADOS DA CONEXÃO (AJUSTE A SENHA) ====
server = "sql-pregao-final.database.windows.net"
database = "pregao"
user = "sqladmin"
password = "Camila1412*"  # coloque a MESMA senha que você digitou ao criar o servidor

print(f"Conectando em: {server} | banco: {database} | usuário: {user}")

try:
    conn = pytds.connect(
        server=server,
        database=database,
        user=user,
        password=password,
        port=1433,
        cafile=certifi.where(),  # certificados raiz
        validate_host=False,     # não checa estritamente o host do certificado
    )

    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    print("Conexão OK! Resultado do SELECT 1:", row)

    cursor.close()
    conn.close()
    print("Conexão encerrada com sucesso.")

except Exception as e:
    print("ERRO AO CONECTAR:")
    print(repr(e))
