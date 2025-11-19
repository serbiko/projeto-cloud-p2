"""
API FastAPI para consulta de dados de pregão da B3
"""
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional
from datetime import date
import math

import crud
import models
import schemas
from database import engine, get_db
# Criar tabelas (se não existirem)
models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="API de Ativos B3",
    description="API para consulta de dados de pregão da B3",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS - Permitir requisições do frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especificar domínios permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """
    Endpoint raiz - informações da API
    """
    return {
        "message": "API de Ativos B3",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health"
    }


@app.get("/api/health")
async def health_check():
    """
    Health check - verifica se a API está funcionando
    """
    return {
        "status": "ok",
        "service": "api-ativos-b3"
    }


@app.get("/api/assets", response_model=schemas.AssetListResponse)
async def list_assets(
    q: Optional[str] = Query(None, description="Ticker para buscar (ex: PETR4)"),
    data_inicio: Optional[date] = Query(None, alias="from", description="Data inicial (formato: YYYY-MM-DD)"),
    data_fim: Optional[date] = Query(None, alias="to", description="Data final (formato: YYYY-MM-DD)"),
    page: int = Query(0, ge=0, description="Número da página (começa em 0)"),
    size: int = Query(30, ge=1, le=100, description="Quantidade de registros por página"),
    db: Session = Depends(get_db)
):
    """
    Lista ativos com filtros e paginação
    
    - **q**: Ticker para filtrar (busca parcial, case-insensitive)
    - **from**: Data inicial do período
    - **to**: Data final do período
    - **page**: Número da página (começa em 0)
    - **size**: Quantidade de registros por página (máximo 100)
    """
    skip = page * size
    
    try:
        assets, total = crud.get_assets(
            db=db,
            ticker=q,
            data_inicio=data_inicio,
            data_fim=data_fim,
            skip=skip,
            limit=size
        )
        
        total_pages = math.ceil(total / size) if total > 0 else 0
        
        return {
            "content": assets,
            "total": total,
            "page": page,
            "size": size,
            "total_pages": total_pages
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar ativos: {str(e)}")


@app.get("/api/assets/{asset_id}", response_model=schemas.Asset)
async def get_asset(
    asset_id: int,
    db: Session = Depends(get_db)
):
    """
    Busca um ativo específico por ID
    
    - **asset_id**: ID do ativo no banco de dados
    """
    asset = crud.get_asset_by_id(db, asset_id)
    if not asset:
        raise HTTPException(
            status_code=404,
            detail=f"Ativo com ID {asset_id} não encontrado"
        )
    return asset


@app.get("/api/assets/meta/tickers")
async def get_tickers(db: Session = Depends(get_db)):
    """
    Lista todos os tickers disponíveis no banco
    """
    try:
        tickers = crud.get_available_tickers(db)
        return {
            "tickers": [t[0] for t in tickers],
            "total": len(tickers)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar tickers: {str(e)}")


@app.get("/api/assets/meta/dates")
async def get_dates(
    limit: int = Query(30, ge=1, le=365, description="Quantidade de datas a retornar"),
    db: Session = Depends(get_db)
):
    """
    Lista as datas disponíveis no banco (últimas N datas)
    
    - **limit**: Quantidade de datas a retornar (máximo 365)
    """
    try:
        dates = crud.get_available_dates(db, limit)
        return {
            "dates": [d[0].isoformat() for d in dates],
            "total": len(dates)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar datas: {str(e)}")


@app.get("/api/assets/ticker/{ticker}")
async def get_assets_by_ticker(
    ticker: str,
    limit: int = Query(30, ge=1, le=365, description="Quantidade de registros"),
    db: Session = Depends(get_db)
):
    """
    Busca histórico de um ticker específico
    
    - **ticker**: Código do ticker (ex: PETR4)
    - **limit**: Quantidade de registros a retornar
    """
    try:
        assets = crud.get_assets_by_ticker(db, ticker, limit)
        if not assets:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum registro encontrado para o ticker {ticker}"
            )
        return {
            "ticker": ticker,
            "data": assets,
            "total": len(assets)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar ticker: {str(e)}")