"""
Operações CRUD (Create, Read, Update, Delete) no banco de dados
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from . import models
from datetime import date
from typing import Optional, Tuple, List


def get_assets(
    db: Session,
    ticker: Optional[str] = None,
    data_inicio: Optional[date] = None,
    data_fim: Optional[date] = None,
    skip: int = 0,
    limit: int = 30
) -> Tuple[List[models.Asset], int]:
    """
    Busca ativos com filtros e paginação
    
    Args:
        db: Sessão do banco de dados
        ticker: Ticker para filtrar (busca parcial, case-insensitive)
        data_inicio: Data inicial do período
        data_fim: Data final do período
        skip: Quantidade de registros a pular (para paginação)
        limit: Quantidade máxima de registros a retornar
    
    Returns:
        Tupla (lista de assets, total de registros)
    """
    query = db.query(models.Asset)
    
    # Aplicar filtros
    filters = []
    
    if ticker:
        # Busca case-insensitive
        filters.append(models.Asset.ticker.ilike(f"%{ticker}%"))
    
    if data_inicio:
        filters.append(models.Asset.data_pregao >= data_inicio)
    
    if data_fim:
        filters.append(models.Asset.data_pregao <= data_fim)
    
    if filters:
        query = query.filter(and_(*filters))
    
    # Contar total de registros (antes da paginação)
    total = query.count()
    
    # Aplicar ordenação e paginação
    assets = (
        query
        .order_by(
            models.Asset.data_pregao.desc(),
            models.Asset.ticker
        )
        .offset(skip)
        .limit(limit)
        .all()
    )
    
    return assets, total


def get_asset_by_id(db: Session, asset_id: int) -> Optional[models.Asset]:
    """
    Busca um ativo por ID
    
    Args:
        db: Sessão do banco de dados
        asset_id: ID do ativo
    
    Returns:
        Asset ou None se não encontrado
    """
    return db.query(models.Asset).filter(models.Asset.id == asset_id).first()


def get_available_tickers(db: Session) -> List:
    """
    Retorna lista de todos os tickers disponíveis
    
    Args:
        db: Sessão do banco de dados
    
    Returns:
        Lista de tuplas com tickers únicos
    """
    return (
        db.query(models.Asset.ticker)
        .distinct()
        .order_by(models.Asset.ticker)
        .all()
    )


def get_available_dates(db: Session, limit: int = 30) -> List:
    """
    Retorna as últimas N datas disponíveis
    
    Args:
        db: Sessão do banco de dados
        limit: Quantidade de datas a retornar
    
    Returns:
        Lista de tuplas com datas únicas
    """
    return (
        db.query(models.Asset.data_pregao)
        .distinct()
        .order_by(models.Asset.data_pregao.desc())
        .limit(limit)
        .all()
    )


def get_assets_by_ticker(
    db: Session,
    ticker: str,
    limit: int = 30
) -> List[models.Asset]:
    """
    Busca histórico de um ticker específico
    
    Args:
        db: Sessão do banco de dados
        ticker: Código do ticker
        limit: Quantidade de registros
    
    Returns:
        Lista de assets do ticker
    """
    return (
        db.query(models.Asset)
        .filter(models.Asset.ticker == ticker.upper())
        .order_by(models.Asset.data_pregao.desc())
        .limit(limit)
        .all()
    )