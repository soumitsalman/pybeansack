__author__ = "Soumit Salman Rahman"
__license__ = "MIT"
__version__ = "1.0.0"

__all__ = [
    'models', 
    'Bean', 'Chatter', 'Publisher', "TrendingBean", "AggregatedBean", 
    'Beansack', 'DuckDB', 'DuckSack', 'LanceSack', 'PGSack', 
    'SimpleVectorDB', 'AsyncCDNStore',
    "create_client", "create_db",
    "BEANS", "PUBLISHERS", "CHATTERS", "RELATED_BEANS", "DATETIME"
] 

from typing import Literal
from .duckdbsack import DuckDB
from .ducklakesack import DuckSack
from .lancesack import LanceSack
from .pgsack import PGSack
from .models import *
from .simplevectordb import *
from .cdnstore import *
from .utils import *
from .database import *

DB_TYPE = Literal["duckdb", "duck", "lancedb", "lance", "ducklake", "dl", "postgres", "postgresql", "pg"]

def create_client(db_type: DB_TYPE, **connection_kwargs) -> Beansack:
    if db_type in ["postgres", "postgresql", "pg"]: return PGSack(connection_kwargs['pg_connection_string'])
    if db_type in ["lancedb", "lance"]: return LanceSack(connection_kwargs['lancedb_storage'])
    if db_type in ["duckdb", "duck"]: return DuckDB(connection_kwargs['duckdb_storage'])
    if db_type in ["ducklake", "dl"]: return DuckSack(catalogdb=connection_kwargs['ducklake_catalog'], storagedb=connection_kwargs['ducklake_storage'])
    raise ValueError("unsupported connection string")

def create_db(db_type: DB_TYPE, **connection_kwargs) -> Beansack:
    if db_type in ["pg", "postgres", "postgresql"]: return pgsack.create_db(connection_kwargs['pg_connection_string'])
    if db_type in ["lancedb", "lance"]: return lancesack.create_db(connection_kwargs['lancedb_storage'])
    if db_type in ["duckdb", "duck"]: return duckdbsack.create_db(connection_kwargs['duckdb_storage'])    
    if db_type in ["ducklake", "dl"]: return ducklakesack.create_db(connection_kwargs['ducklake_catalog'], connection_kwargs['ducklake_storage'])
    raise ValueError("unsupported db type")
