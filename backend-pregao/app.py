"""
API Backend para Consulta de Ativos - Dados de Pregão B3
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import pymssql
import os
from datetime import datetime, timedelta
from functools import wraps

app = Flask(__name__)
CORS(app)  # Permitir requisições do frontend

# --------------------------------------------------
# CONFIGURAÇÃO DO BANCO DE DADOS
# --------------------------------------------------
def get_db_connection():
    """
    Estabelece conexão com SQL Server usando variáveis de ambiente.
    """
    server = os.getenv("SQL_SERVER", "sql-pregao-final.database.windows.net")
    database = os.getenv("SQL_DATABASE", "pregao")
    user = os.getenv("SQL_USER", "sqladmin")
    password = os.getenv("SQL_PASSWORD", "Camila1412*")
    
    try:
        conn = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            login_timeout=30,
            timeout=30
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco: {str(e)}")
        raise


# --------------------------------------------------
# DECORATOR PARA TRATAMENTO DE ERROS
# --------------------------------------------------
def handle_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            return jsonify({
                'error': True,
                'message': str(e)
            }), 500
    return decorated_function


# --------------------------------------------------
# ENDPOINTS DA API
# --------------------------------------------------

@app.route('/')
def index():
    """
    Endpoint raiz - Documentação da API
    """
    return jsonify({
        'api': 'API de Consulta de Ativos B3',
        'version': '1.0',
        'endpoints': {
            '/': 'Documentação da API',
            '/health': 'Status da API e conexão com banco',
            '/api/tickers': 'Lista todos os tickers disponíveis',
            '/api/datas': 'Lista todas as datas disponíveis',
            '/api/ativo/<ticker>': 'Consulta dados de um ativo específico',
            '/api/ativo/<ticker>/historico': 'Histórico completo de um ativo',
            '/api/cotacao': 'Cotações por data (query params: data, ticker)',
            '/api/top-volume': 'Top 10 ativos por volume (query param: data)',
            '/api/resumo': 'Resumo do mercado por data (query param: data)'
        }
    })


@app.route('/health')
@handle_errors
def health():
    """
    Verifica status da API e conexão com banco
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Testar conexão
    cursor.execute("SELECT COUNT(*) FROM dbo.DadosPregao")
    total_registros = cursor.fetchone()[0]
    
    # Última data disponível
    cursor.execute("""
        SELECT MAX(data_pregao) 
        FROM dbo.DadosPregao
    """)
    ultima_data = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'status': 'ok',
        'database': 'connected',
        'total_registros': total_registros,
        'ultima_data': ultima_data.strftime('%Y-%m-%d') if ultima_data else None,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/tickers')
@handle_errors
def get_tickers():
    """
    Lista todos os tickers disponíveis
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT ticker
        FROM dbo.DadosPregao
        ORDER BY ticker
    """)
    
    tickers = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'total': len(tickers),
        'tickers': tickers
    })


@app.route('/api/datas')
@handle_errors
def get_datas():
    """
    Lista todas as datas disponíveis
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT data_pregao
        FROM dbo.DadosPregao
        ORDER BY data_pregao DESC
    """)
    
    datas = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'total': len(datas),
        'datas': datas
    })


@app.route('/api/ativo/<ticker>')
@handle_errors
def get_ativo(ticker):
    """
    Consulta dados mais recentes de um ativo específico
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Buscar dados mais recentes do ticker
    cursor.execute("""
        SELECT TOP 1
            ticker,
            data_pregao,
            preco_abertura,
            preco_min,
            preco_max,
            preco_medio,
            preco_ultimo,
            quantidade_negociada,
            data_insercao
        FROM dbo.DadosPregao
        WHERE ticker = %s
        ORDER BY data_pregao DESC
    """, (ticker.upper(),))
    
    row = cursor.fetchone()
    
    if not row:
        cursor.close()
        conn.close()
        return jsonify({
            'error': True,
            'message': f'Ticker {ticker} não encontrado'
        }), 404
    
    resultado = {
        'ticker': row[0],
        'data_pregao': row[1].strftime('%Y-%m-%d'),
        'preco_abertura': float(row[2]),
        'preco_minimo': float(row[3]),
        'preco_maximo': float(row[4]),
        'preco_medio': float(row[5]),
        'preco_ultimo': float(row[6]),
        'quantidade_negociada': int(row[7]),
        'data_insercao': row[8].strftime('%Y-%m-%d %H:%M:%S')
    }
    
    cursor.close()
    conn.close()
    
    return jsonify(resultado)


@app.route('/api/ativo/<ticker>/historico')
@handle_errors
def get_historico(ticker):
    """
    Histórico completo de um ativo
    Query params:
    - limit: número de registros (default: 30)
    """
    limit = request.args.get('limit', 30, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT TOP {limit}
            ticker,
            data_pregao,
            preco_abertura,
            preco_min,
            preco_max,
            preco_medio,
            preco_ultimo,
            quantidade_negociada
        FROM dbo.DadosPregao
        WHERE ticker = %s
        ORDER BY data_pregao DESC
    """, (ticker.upper(),))
    
    historico = []
    for row in cursor.fetchall():
        historico.append({
            'ticker': row[0],
            'data': row[1].strftime('%Y-%m-%d'),
            'abertura': float(row[2]),
            'minimo': float(row[3]),
            'maximo': float(row[4]),
            'medio': float(row[5]),
            'fechamento': float(row[6]),
            'volume': int(row[7])
        })
    
    cursor.close()
    conn.close()
    
    if not historico:
        return jsonify({
            'error': True,
            'message': f'Nenhum dado encontrado para {ticker}'
        }), 404
    
    return jsonify({
        'ticker': ticker.upper(),
        'total_registros': len(historico),
        'historico': historico
    })


@app.route('/api/cotacao')
@handle_errors
def get_cotacao():
    """
    Consulta cotações por data e/ou ticker
    Query params:
    - data: data no formato YYYY-MM-DD
    - ticker: código do ativo (opcional)
    """
    data = request.args.get('data')
    ticker = request.args.get('ticker')
    
    if not data:
        return jsonify({
            'error': True,
            'message': 'Parâmetro "data" é obrigatório (formato: YYYY-MM-DD)'
        }), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if ticker:
        # Consulta específica para um ticker
        cursor.execute("""
            SELECT 
                ticker,
                data_pregao,
                preco_abertura,
                preco_min,
                preco_max,
                preco_medio,
                preco_ultimo,
                quantidade_negociada
            FROM dbo.DadosPregao
            WHERE data_pregao = %s
            AND ticker = %s
        """, (data, ticker.upper()))
    else:
        # Consulta todos os tickers da data
        cursor.execute("""
            SELECT 
                ticker,
                data_pregao,
                preco_abertura,
                preco_min,
                preco_max,
                preco_medio,
                preco_ultimo,
                quantidade_negociada
            FROM dbo.DadosPregao
            WHERE data_pregao = %s
            ORDER BY ticker
        """, (data,))
    
    cotacoes = []
    for row in cursor.fetchall():
        cotacoes.append({
            'ticker': row[0],
            'data': row[1].strftime('%Y-%m-%d'),
            'abertura': float(row[2]),
            'minimo': float(row[3]),
            'maximo': float(row[4]),
            'medio': float(row[5]),
            'fechamento': float(row[6]),
            'volume': int(row[7])
        })
    
    cursor.close()
    conn.close()
    
    if not cotacoes:
        return jsonify({
            'error': True,
            'message': f'Nenhuma cotação encontrada para a data {data}'
        }), 404
    
    return jsonify({
        'data': data,
        'ticker': ticker.upper() if ticker else 'todos',
        'total': len(cotacoes),
        'cotacoes': cotacoes
    })


@app.route('/api/top-volume')
@handle_errors
def get_top_volume():
    """
    Top 10 ativos por volume
    Query param:
    - data: data no formato YYYY-MM-DD (opcional, usa última data se não informado)
    """
    data = request.args.get('data')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if not data:
        # Buscar última data disponível
        cursor.execute("SELECT MAX(data_pregao) FROM dbo.DadosPregao")
        data = cursor.fetchone()[0].strftime('%Y-%m-%d')
    
    cursor.execute("""
        SELECT TOP 10
            ticker,
            preco_ultimo,
            quantidade_negociada,
            (preco_ultimo * quantidade_negociada) as volume_financeiro
        FROM dbo.DadosPregao
        WHERE data_pregao = %s
        ORDER BY quantidade_negociada DESC
    """, (data,))
    
    top_ativos = []
    for row in cursor.fetchall():
        top_ativos.append({
            'ticker': row[0],
            'preco': float(row[1]),
            'quantidade': int(row[2]),
            'volume_financeiro': float(row[3])
        })
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'data': data,
        'top_10_volume': top_ativos
    })


@app.route('/api/resumo')
@handle_errors
def get_resumo():
    """
    Resumo do mercado por data
    Query param:
    - data: data no formato YYYY-MM-DD (opcional, usa última data se não informado)
    """
    data = request.args.get('data')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if not data:
        # Buscar última data disponível
        cursor.execute("SELECT MAX(data_pregao) FROM dbo.DadosPregao")
        data = cursor.fetchone()[0].strftime('%Y-%m-%d')
    
    # Estatísticas gerais
    cursor.execute("""
        SELECT 
            COUNT(*) as total_ativos,
            SUM(quantidade_negociada) as volume_total,
            AVG(preco_ultimo) as preco_medio,
            MAX(preco_ultimo) as maior_preco,
            MIN(preco_ultimo) as menor_preco
        FROM dbo.DadosPregao
        WHERE data_pregao = %s
    """, (data,))
    
    row = cursor.fetchone()
    
    resumo = {
        'data': data,
        'total_ativos_negociados': int(row[0]),
        'volume_total': int(row[1]),
        'preco_medio': float(row[2]),
        'maior_preco': float(row[3]),
        'menor_preco': float(row[4])
    }
    
    cursor.close()
    conn.close()
    
    return jsonify(resumo)


# --------------------------------------------------
# EXECUTAR APLICAÇÃO
# --------------------------------------------------
if __name__ == '__main__':
    # Para desenvolvimento local
    app.run(debug=True, host='0.0.0.0', port=5000)