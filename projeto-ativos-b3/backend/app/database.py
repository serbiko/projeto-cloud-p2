"""
Configuração do banco de dados SQLAlchemy
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from urllib.parse import quote_plus

# Variáveis de ambiente (vamos configurar no próximo passo)
SQL_SERVER = os.getenv("SQL_SERVER", "sql-pregao-final.database.windows.net")
SQL_DATABASE = os.getenv("SQL_DATABASE", "pregao")
SQL_USER = os.getenv("SQL_USER", "sqladmin")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "Camila1412*")

# Construir connection string
# Formato: mssql+pymssql://user:password@server/database
password_encoded = quote_plus(SQL_PASSWORD)
DATABASE_URL = f"mssql+pymssql://{SQL_USER}:{password_encoded}@{SQL_SERVER}/{SQL_DATABASE}"

# Criar engine
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Colocar True para debug (mostra queries SQL)
    pool_pre_ping=True,  # Verifica conexões antes de usar
    pool_size=5,  # Número de conexões no pool
    max_overflow=10,  # Máximo de conexões extras
    pool_recycle=3600,  # Reciclar conexões a cada 1 hora
)

# Criar SessionLocal
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db():
    """
    Dependency para obter sessão do banco de dados
    Uso: db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """
    Testa a conexão com o banco de dados
    """
    from sqlalchemy import text
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        print("✓ Conexão com banco de dados OK")
        return True
    except Exception as e:
        print(f"✗ Erro ao conectar no banco: {e}")
        return False



if __name__ == "__main__":
    # Testar conexão
    test_connection()