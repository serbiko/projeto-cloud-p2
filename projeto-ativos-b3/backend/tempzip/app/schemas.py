"""
Schemas Pydantic para validação e serialização de dados
"""
from pydantic import BaseModel, Field
from datetime import date
from typing import List


class AssetBase(BaseModel):
    """Schema base para Asset"""
    ticker: str = Field(..., description="Código do ticker (ex: PETR4)")
    data_pregao: date = Field(..., description="Data do pregão")
    preco_abertura: float = Field(..., description="Preço de abertura", ge=0)
    preco_min: float = Field(..., description="Preço mínimo do dia", ge=0)
    preco_max: float = Field(..., description="Preço máximo do dia", ge=0)
    preco_medio: float = Field(..., description="Preço médio do dia", ge=0)
    preco_ultimo: float = Field(..., description="Preço de fechamento", ge=0)
    quantidade_negociada: int = Field(..., description="Quantidade negociada", ge=0)


class Asset(AssetBase):
    """Schema completo para Asset (inclui ID)"""
    id: int = Field(..., description="ID único do registro")
    
    class Config:
        from_attributes = True  # Permite converter de modelo SQLAlchemy
        json_schema_extra = {
            "example": {
                "id": 1,
                "ticker": "PETR4",
                "data_pregao": "2024-11-18",
                "preco_abertura": 42.50,
                "preco_min": 42.10,
                "preco_max": 43.20,
                "preco_medio": 42.75,
                "preco_ultimo": 43.00,
                "quantidade_negociada": 15000000
            }
        }


class AssetListResponse(BaseModel):
    """Schema para resposta paginada de assets"""
    content: List[Asset] = Field(..., description="Lista de ativos")
    total: int = Field(..., description="Total de registros", ge=0)
    page: int = Field(..., description="Página atual", ge=0)
    size: int = Field(..., description="Tamanho da página", ge=1)
    total_pages: int = Field(..., description="Total de páginas", ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "content": [
                    {
                        "id": 1,
                        "ticker": "PETR4",
                        "data_pregao": "2024-11-18",
                        "preco_abertura": 42.50,
                        "preco_min": 42.10,
                        "preco_max": 43.20,
                        "preco_medio": 42.75,
                        "preco_ultimo": 43.00,
                        "quantidade_negociada": 15000000
                    }
                ],
                "total": 150,
                "page": 0,
                "size": 30,
                "total_pages": 5
            }
        }