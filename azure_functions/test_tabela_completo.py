"""
Script de teste para validar:
1. Conexão com SQL Server
2. Existência da tabela DadosPregao
3. Estrutura correta da tabela
4. Inserção e consulta de dados de teste
"""

import pymssql
import os
from datetime import datetime, date

# ====================================================
# CONFIGURAÇÕES - Ajuste conforme suas variáveis
# ====================================================
SQL_CONFIG = {
    'server': os.getenv("SQL_SERVER", "sql-pregao-final.database.windows.net"),
    'database': os.getenv("SQL_DATABASE", "pregao"),
    'user': os.getenv("SQL_USER", "sqladmin"),
    'password': os.getenv("SQL_PASSWORD", "Camila1412*")
}

print("=" * 70)
print("TESTE DE CONEXÃO E VALIDAÇÃO DA TABELA DadosPregao")
print("=" * 70)

# ====================================================
# TESTE 1: Conexão com o banco
# ====================================================
print("\n[TESTE 1] Conectando ao SQL Server...")
print(f"Server: {SQL_CONFIG['server']}")
print(f"Database: {SQL_CONFIG['database']}")
print(f"User: {SQL_CONFIG['user']}")

try:
    conn = pymssql.connect(
        server=SQL_CONFIG['server'],
        user=SQL_CONFIG['user'],
        password=SQL_CONFIG['password'],
        database=SQL_CONFIG['database'],
        login_timeout=30,
        timeout=30
    )
    print("✓ Conexão estabelecida com sucesso!\n")
except Exception as e:
    print(f"✗ ERRO ao conectar: {str(e)}")
    exit(1)

cursor = conn.cursor()

# ====================================================
# TESTE 2: Verificar se a tabela existe
# ====================================================
print("[TESTE 2] Verificando existência da tabela...")

check_table_sql = """
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'DadosPregao'
"""

cursor.execute(check_table_sql)
result = cursor.fetchone()

if result[0] == 0:
    print("✗ Tabela DadosPregao NÃO EXISTE!")
    print("\nExecute o script 'criar_tabela_dados_pregao.sql' antes de continuar.")
    cursor.close()
    conn.close()
    exit(1)
else:
    print("✓ Tabela DadosPregao existe!\n")

# ====================================================
# TESTE 3: Verificar estrutura da tabela
# ====================================================
print("[TESTE 3] Verificando estrutura da tabela...")

structure_sql = """
    SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'DadosPregao'
    ORDER BY ORDINAL_POSITION
"""

cursor.execute(structure_sql)
columns = cursor.fetchall()

print("\nEstrutura da tabela:")
print("-" * 70)
print(f"{'Coluna':<30} {'Tipo':<20} {'Tamanho':<10} {'Nulo':<5}")
print("-" * 70)

for col in columns:
    col_name, data_type, max_length, is_nullable = col
    max_length_str = str(max_length) if max_length else "N/A"
    is_nullable_str = "SIM" if is_nullable == "YES" else "NÃO"
    print(f"{col_name:<30} {data_type:<20} {max_length_str:<10} {is_nullable_str:<5}")

print("-" * 70)

# ====================================================
# TESTE 4: Verificar índices
# ====================================================
print("\n[TESTE 4] Verificando índices...")

indexes_sql = """
    SELECT 
        i.name AS index_name,
        i.type_desc,
        COL_NAME(ic.object_id, ic.column_id) AS column_name
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic 
        ON i.object_id = ic.object_id 
        AND i.index_id = ic.index_id
    WHERE i.object_id = OBJECT_ID('dbo.DadosPregao')
    ORDER BY i.name, ic.key_ordinal
"""

cursor.execute(indexes_sql)
indexes = cursor.fetchall()

if indexes:
    print("\nÍndices encontrados:")
    current_index = None
    for idx in indexes:
        index_name, index_type, column_name = idx
        if index_name != current_index:
            print(f"\n  • {index_name} ({index_type})")
            current_index = index_name
        print(f"    - {column_name}")
else:
    print("⚠ Nenhum índice encontrado")

# ====================================================
# TESTE 5: Contar registros existentes
# ====================================================
print("\n[TESTE 5] Contando registros existentes...")

count_sql = "SELECT COUNT(*) FROM dbo.DadosPregao"
cursor.execute(count_sql)
count = cursor.fetchone()[0]

print(f"Total de registros na tabela: {count:,}")

# ====================================================
# TESTE 6: Inserir registro de teste
# ====================================================
print("\n[TESTE 6] Testando inserção de registro...")

# Limpar possível registro de teste anterior
cursor.execute("DELETE FROM dbo.DadosPregao WHERE ticker = 'TEST'")
conn.commit()

# Inserir registro de teste
test_data = (
    'TEST',
    date.today(),
    10.50,
    9.80,
    11.20,
    10.45,
    10.60,
    1000
)

insert_sql = """
    INSERT INTO dbo.DadosPregao (
        ticker, data_pregao,
        preco_abertura, preco_min, preco_max,
        preco_medio, preco_ultimo,
        quantidade_negociada
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

try:
    cursor.execute(insert_sql, test_data)
    conn.commit()
    print("✓ Registro de teste inserido com sucesso!")
    
    # Consultar o registro inserido
    cursor.execute("SELECT * FROM dbo.DadosPregao WHERE ticker = 'TEST'")
    result = cursor.fetchone()
    
    if result:
        print("\nRegistro inserido:")
        print(f"  ID: {result[0]}")
        print(f"  Ticker: {result[1]}")
        print(f"  Data: {result[2]}")
        print(f"  Abertura: R$ {result[3]:.2f}")
        print(f"  Mínimo: R$ {result[4]:.2f}")
        print(f"  Máximo: R$ {result[5]:.2f}")
        print(f"  Médio: R$ {result[6]:.2f}")
        print(f"  Último: R$ {result[7]:.2f}")
        print(f"  Quantidade: {result[8]:,}")
    
    # Remover registro de teste
    cursor.execute("DELETE FROM dbo.DadosPregao WHERE ticker = 'TEST'")
    conn.commit()
    print("\n✓ Registro de teste removido")
    
except Exception as e:
    print(f"✗ ERRO ao inserir registro de teste: {str(e)}")
    conn.rollback()

# ====================================================
# TESTE 7: Últimos registros (se houver)
# ====================================================
if count > 0:
    print("\n[TESTE 7] Consultando últimos 5 registros...")
    
    query_sql = """
        SELECT TOP 5
            ticker, 
            data_pregao,
            preco_ultimo,
            quantidade_negociada
        FROM dbo.DadosPregao
        ORDER BY data_pregao DESC, ticker
    """
    
    cursor.execute(query_sql)
    results = cursor.fetchall()
    
    print("\nÚltimos registros:")
    print("-" * 70)
    print(f"{'Ticker':<10} {'Data':<12} {'Preço Último':>15} {'Quantidade':>15}")
    print("-" * 70)
    
    for row in results:
        ticker, data, preco, qtd = row
        print(f"{ticker:<10} {str(data):<12} R$ {preco:>12,.2f} {qtd:>15,}")
    
    print("-" * 70)

# ====================================================
# Finalizar
# ====================================================
cursor.close()
conn.close()

print("\n" + "=" * 70)
print("TODOS OS TESTES CONCLUÍDOS!")
print("=" * 70)
print("\n✓ A tabela está pronta para uso.")
print("✓ Você pode agora fazer o deploy da Azure Function.")