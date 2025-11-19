"""
Models SQLAlchemy para a tabela DadosPregao
"""
from sqlalchemy import Column, Integer, String, Float, Date, BigInteger, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Asset(Base):
    """
    Model para a tabela DadosPregao
    Representa os dados de pregão de um ativo da B3
    """
    __tablename__ = "DadosPregao"
    
    # Colunas
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ticker = Column(String(20), nullable=False, index=True)
    data_pregao = Column(Date, nullable=False, index=True)
    preco_abertura = Column(Float, nullable=False)
    preco_min = Column(Float, nullable=False)
    preco_max = Column(Float, nullable=False)
    preco_medio = Column(Float, nullable=False)
    preco_ultimo = Column(Float, nullable=False)
    quantidade_negociada = Column(BigInteger, nullable=False)
    data_insercao = Column(DateTime, default=datetime.utcnow)
    
    # Índice composto para melhorar performance de buscas
    __table_args__ = (
        Index('idx_ticker_data', 'ticker', 'data_pregao'),
    )
    
    def __repr__(self):
        return f"<Asset(ticker='{self.ticker}', data='{self.data_pregao}', preco={self.preco_ultimo})>"