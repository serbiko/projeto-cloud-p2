import logging
import azure.functions as func
import pymssql
import xml.etree.ElementTree as ET
import os
import re
from datetime import datetime
from typing import List, Tuple

# --------------------------------------------------
# 1) CONEXÃO COM SQL SERVER (USANDO pymssql)
# --------------------------------------------------
def get_sql_connection():
    """
    Estabelece conexão com SQL Server usando variáveis de ambiente.
    Retorna a conexão ou lança exceção em caso de erro.
    """
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")

    # Validar se todas as variáveis estão presentes
    if not all([server, database, user, password]):
        missing = [k for k, v in {
            "SQL_SERVER": server,
            "SQL_DATABASE": database,
            "SQL_USER": user,
            "SQL_PASSWORD": password
        }.items() if not v]
        raise ValueError(f"Variáveis de ambiente não configuradas: {', '.join(missing)}")

    logging.info(f"[SQL] Conectando em {server}, DB={database}, User={user}")

    try:
        conn = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            login_timeout=30,
            timeout=60  # Aumentado para 60s devido ao volume de dados
        )
        logging.info("[SQL] Conexão estabelecida com sucesso")
        return conn
    except Exception as e:
        logging.error(f"[SQL] Erro ao conectar: {str(e)}")
        raise


# --------------------------------------------------
# 2) VERIFICAR/CRIAR TABELA
# --------------------------------------------------
def verificar_tabela(conn):
    """
    Verifica se a tabela existe, caso contrário cria.
    """
    cursor = conn.cursor()
    
    # Verificar se a tabela existe
    check_sql = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND TABLE_NAME = 'DadosPregao'
    """
    
    try:
        cursor.execute(check_sql)
        result = cursor.fetchone()
        
        if result[0] == 0:
            logging.warning("[SQL] Tabela DadosPregao não existe. Criando...")
            
            create_sql = """
                CREATE TABLE dbo.DadosPregao (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ticker VARCHAR(20) NOT NULL,
                    data_pregao DATE NOT NULL,
                    preco_abertura FLOAT NOT NULL,
                    preco_min FLOAT NOT NULL,
                    preco_max FLOAT NOT NULL,
                    preco_medio FLOAT NOT NULL,
                    preco_ultimo FLOAT NOT NULL,
                    quantidade_negociada BIGINT NOT NULL,
                    data_insercao DATETIME DEFAULT GETDATE(),
                    INDEX idx_ticker_data (ticker, data_pregao)
                )
            """
            cursor.execute(create_sql)
            conn.commit()
            logging.info("[SQL] Tabela DadosPregao criada com sucesso")
        else:
            logging.info("[SQL] Tabela DadosPregao já existe")
            
    except Exception as e:
        logging.error(f"[SQL] Erro ao verificar/criar tabela: {str(e)}")
        raise
    finally:
        cursor.close()


# --------------------------------------------------
# 3) PARSE DO XML DO TIPO BVMF.217.01.xsd
# --------------------------------------------------
def extrair_registros(xml_string):
    """
    Extrai registros do XML da B3 (formato BVMF.217.01.xsd)
    Filtra APENAS tickers que terminam com: 3, 4, 5, 6, 11, 34
    """
    import re
    
    ns = {"b": "urn:bvmf.217.01.xsd"}
    root = ET.fromstring(xml_string)

    registros = []
    validos = 0
    invalidos = 0

    # REGEX CORRETO: Apenas terminações válidas
    # ^[A-Z]{4}(3|4|5|6|11|34)$
    # Aceita: PETR3, VALE4, ITUB5, BBDC6, TAEE11, PETR34
    # Rejeita: PETR35, VALE39, etc
    pattern_valido = re.compile(r'^[A-Z]{4}(3|4|5|6|11|34)$')

    for pr in root.findall(".//b:PricRpt", ns):
        ticker = pr.findtext("b:SctyId/b:TckrSymb", default="", namespaces=ns).strip()
        
        # Filtrar apenas tickers válidos
        if not pattern_valido.match(ticker):
            invalidos += 1
            continue
        
        data = pr.findtext("b:TradDt/b:Dt", default="", namespaces=ns)
        preco_abertura = pr.findtext("b:FinInstrmAttrbts/b:FrstPric", default="0", namespaces=ns)
        preco_min = pr.findtext("b:FinInstrmAttrbts/b:MinPric", default="0", namespaces=ns)
        preco_max = pr.findtext("b:FinInstrmAttrbts/b:MaxPric", default="0", namespaces=ns)
        preco_medio = pr.findtext("b:FinInstrmAttrbts/b:TradAvrgPric", default="0", namespaces=ns)
        preco_ultimo = pr.findtext("b:FinInstrmAttrbts/b:LastPric", default="0", namespaces=ns)
        quantidade = pr.findtext("b:FinInstrmAttrbts/b:RglrTxsQty", default="0", namespaces=ns)

        registros.append((
            ticker,
            data,
            float(preco_abertura),
            float(preco_min),
            float(preco_max),
            float(preco_medio),
            float(preco_ultimo),
            int(quantidade)
        ))
        validos += 1

    logging.info(f"[XML] Registros válidos extraídos: {validos}")
    logging.warning(f"[XML] Registros inválidos ignorados: {invalidos}")
    logging.info(f"[2/3] ✓ Total de registros extraídos: {validos:,}")

    return registros


# --------------------------------------------------
# 4) LIMPAR DADOS DUPLICADOS (OPCIONAL)
# --------------------------------------------------
def limpar_duplicados(conn, data_pregao):
    """
    Remove registros duplicados para a mesma data antes de inserir novos.
    """
    cursor = conn.cursor()
    
    try:
        delete_sql = "DELETE FROM dbo.DadosPregao WHERE data_pregao = %s"
        cursor.execute(delete_sql, (data_pregao,))
        deleted = cursor.rowcount
        conn.commit()
        
        if deleted > 0:
            logging.info(f"[SQL] Removidos {deleted} registros duplicados da data {data_pregao}")
            
    except Exception as e:
        logging.error(f"[SQL] Erro ao limpar duplicados: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()


# --------------------------------------------------
# 5) REMOVER DUPLICATAS DO CONJUNTO DE REGISTROS
# --------------------------------------------------
def remover_duplicatas_registros(registros: List[Tuple]) -> List[Tuple]:
    """
    Remove duplicatas do conjunto de registros baseado em ticker + data.
    Mantém apenas o primeiro registro de cada combinação ticker+data.
    """
    vistos = set()
    registros_unicos = []
    duplicatas = 0
    
    for registro in registros:
        ticker = registro[0]
        data = registro[1]
        chave = (ticker, data)
        
        if chave not in vistos:
            vistos.add(chave)
            registros_unicos.append(registro)
        else:
            duplicatas += 1
    
    if duplicatas > 0:
        logging.info(f"[DUPLICATAS] Removidas {duplicatas} duplicatas do XML. Registros únicos: {len(registros_unicos)}")
    
    return registros_unicos


# --------------------------------------------------
# 6) INSERIR NO SQL SERVER EM LOTES
# --------------------------------------------------
def inserir_no_sql(registros: List[Tuple], limpar_antes: bool = True):
    """
    Insere registros no SQL Server em lotes para melhor performance.
    
    Args:
        registros: Lista de tuplas com os dados
        limpar_antes: Se True, remove duplicados da mesma data antes de inserir
    """
    if not registros:
        logging.info("[SQL] Nenhum registro para inserir.")
        return

    conn = None
    cursor = None
    
    try:
        conn = get_sql_connection()
        
        # Verificar/criar tabela
        verificar_tabela(conn)
        
        # Extrair a data do primeiro registro para limpar duplicados
        if limpar_antes and registros:
            data_pregao = registros[0][1]
            limpar_duplicados(conn, data_pregao)
        
        cursor = conn.cursor()

        # SQL de inserção com schema explícito
        insert_sql = """
            INSERT INTO dbo.DadosPregao (
                ticker, data_pregao,
                preco_abertura, preco_min, preco_max,
                preco_medio, preco_ultimo,
                quantidade_negociada
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Processar em lotes de 1000 registros
        batch_size = 1000
        total_inserido = 0
        
        for i in range(0, len(registros), batch_size):
            batch = registros[i:i + batch_size]
            
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserido += len(batch)
                
                logging.info(f"[SQL] Lote {i//batch_size + 1}: {len(batch)} registros inseridos (Total: {total_inserido}/{len(registros)})")
                
            except Exception as e:
                logging.error(f"[SQL] Erro ao inserir lote {i//batch_size + 1}: {str(e)}")
                conn.rollback()
                # Tentar inserir um por um para identificar problemas
                erros = 0
                for registro in batch:
                    try:
                        cursor.execute(insert_sql, registro)
                        conn.commit()
                        total_inserido += 1
                    except Exception as e2:
                        erros += 1
                        logging.error(f"[SQL] Erro ao inserir registro (ticker={registro[0]}): {str(e2)}")
                        conn.rollback()
                
                if erros > 0:
                    logging.warning(f"[SQL] {erros} registros com erro no lote {i//batch_size + 1}")

        logging.info(f"[SQL] ✓ Total inserido com sucesso: {total_inserido} de {len(registros)} registros")
        
    except Exception as e:
        logging.error(f"[SQL] Erro geral ao inserir registros: {str(e)}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("[SQL] Conexão fechada")


# --------------------------------------------------
# 7) FUNÇÃO PRINCIPAL AZURE FUNCTIONS
# --------------------------------------------------
app = func.FunctionApp()

@app.function_name(name="CargaPregaoXml")
@app.blob_trigger(arg_name="myblob", path="pregao-xml/{name}", connection="AzureWebJobsStorage")
def CargaPregaoXml(myblob: func.InputStream):
    """
    Função triggered por blob que processa XML de pregão da B3.
    """
    
    logging.info("=" * 60)
    logging.info("=== Início da Função CargaPregaoXml ===")
    logging.info(f"Arquivo: {myblob.name}")
    logging.info(f"Tamanho: {myblob.length:,} bytes ({myblob.length / 1024 / 1024:.2f} MB)")
    logging.info("=" * 60)

    try:
        # 1) Ler arquivo XML
        logging.info("[1/3] Lendo arquivo XML...")
        xml_bytes = myblob.read()
        
        # Tentar diferentes encodings
        xml_text = None
        for encoding in ['latin1', 'utf-8', 'iso-8859-1']:
            try:
                xml_text = xml_bytes.decode(encoding)
                logging.info(f"[1/3] ✓ Arquivo decodificado com encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if xml_text is None:
            raise ValueError("Não foi possível decodificar o arquivo XML com nenhum encoding conhecido")

        # 2) Extrair registros
        logging.info("[2/3] Extraindo registros do XML...")
        registros = extrair_registros(xml_text)
        
        if not registros:
            logging.warning("[2/3] ⚠ Nenhum registro extraído do XML")
            return
        
        logging.info(f"[2/3] ✓ Total de registros extraídos: {len(registros):,}")

        # Remover duplicatas do próprio XML
        registros = remover_duplicatas_registros(registros)

        # 3) Inserir no banco de dados
        logging.info("[3/3] Inserindo registros no SQL Server...")
        inserir_no_sql(registros, limpar_antes=True)
        logging.info("[3/3] ✓ Registros inseridos com sucesso")

        logging.info("=" * 60)
        logging.info("=== Processo Finalizado com Sucesso ===")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error("=" * 60)
        logging.error(f"=== ERRO NA FUNÇÃO: {str(e)} ===")
        logging.error("=" * 60)
        raise
# ============================================================
# TIME TRIGGER - Download Automático Diário
# ============================================================

@app.schedule(
    schedule="0 0 20 * * *",  # Todo dia às 20h UTC (17h Brasília)
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def DownloadDiarioPregao(myTimer: func.TimerRequest) -> None:
    """
    Time Trigger: Executa automaticamente todo dia às 20h
    Baixa o arquivo XML do pregão do dia da B3
    """
    import logging
    from datetime import datetime, timedelta
    
    logging.info("============================================================")
    logging.info("=== Time Trigger - Download Automático ===")
    logging.info(f"Executando em: {datetime.now()}")
    logging.info("============================================================")
    
    try:
        # Importar a função de download
        from extract import run as download_and_extract
        
        logging.info("[TIME] Iniciando download do XML da B3...")
        
        # Executar download e upload para o Blob
        download_and_extract()
        
        logging.info("[TIME] ✓ Download e upload concluídos com sucesso!")
        logging.info("[TIME] O Blob Trigger irá processar o arquivo automaticamente")
        logging.info("============================================================")
        
    except Exception as e:
        logging.error(f"[TIME] ❌ Erro no download: {str(e)}")
        logging.error("============================================================")
        raise