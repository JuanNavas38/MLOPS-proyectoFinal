"""Helpers de base de datos (psycopg2 + SQLAlchemy engine para pandas)."""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

from pipeline.config import DATABASE_URL

_engine = None


def get_conn():
    """Conexión psycopg2 cruda (para INSERT/UPDATE)."""
    return psycopg2.connect(DATABASE_URL)


def get_engine():
    """Engine SQLAlchemy reutilizable (para pandas.read_sql)."""
    global _engine
    if _engine is None:
        # psycopg2 driver explícito
        url = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


def read_sql(query: str, params=None) -> pd.DataFrame:
    return pd.read_sql(query, get_engine(), params=params)
