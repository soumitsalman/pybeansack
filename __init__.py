__all__ = ['models',  'staticdb', 'cdnstore', 'Beansack', 'Cupboard', 'MongoDB', 'DuckDB', 'Ducklake', 'LanceDB', 'Postgres', "LanceDBCupboard", "create_client", "create_db", "create_cupboard"]  # Specify modules to be exported
__version__ = "0.5.1"

from typing import Literal
from .mongosack import MongoDB
from .duckdbsack import DuckDB
from .ducklakesack import Ducklake
from .lancesack import LanceDB, LanceDBCupboard
from .pgsack import Postgres
from .models import *
from .staticdb import *
from .cdnstore import *
from .utils import *
from .database import *

DB_TYPE = Literal["duckdb", "duck", "lancedb", "lance", "ducklake", "dl", "postgres", "postgresql", "pg"]

def create_cupboard(db_type: DB_TYPE, **connection_kwargs) -> Cupboard:
    # if db_type in ["postgres", "postgresql", "pg"]: return Postgres(connection_kwargs['pg_connection_string'])
    if db_type in ["lancedb", "lance"]: return LanceDBCupboard(connection_kwargs['lancedb_storage'])
    # if db_type in ["duckdb", "duck"]: return DuckDB(connection_kwargs['duckdb_storage'])
    # if db_type in ["ducklake", "dl"]: return Ducklake(catalogdb=connection_kwargs['ducklake_catalog'], storagedb=connection_kwargs['ducklake_storage'])
    raise ValueError("unsupported connection string")

def create_client(db_type: DB_TYPE, **connection_kwargs) -> Beansack:
    if db_type in ["postgres", "postgresql", "pg"]: return Postgres(connection_kwargs['pg_connection_string'])
    if db_type in ["lancedb", "lance"]: return LanceDB(connection_kwargs['lancedb_storage'])
    if db_type in ["duckdb", "duck"]: return DuckDB(connection_kwargs['duckdb_storage'])
    if db_type in ["ducklake", "dl"]: return Ducklake(catalogdb=connection_kwargs['ducklake_catalog'], storagedb=connection_kwargs['ducklake_storage'])
    raise ValueError("unsupported connection string")

def create_db(catalogs_dir: str, db_type: DB_TYPE, **connection_kwargs) -> Beansack:
    if db_type in ["pg", "postgres", "postgresql"]: return pgsack.create_db(connection_kwargs['pg_connection_string'], catalogs_dir)
    if db_type in ["lancedb", "lance"]: return lancesack.create_db(connection_kwargs['lancedb_storage'], catalogs_dir)
    if db_type in ["duckdb", "duck"]: return duckdbsack.create_db(connection_kwargs['duckdb_storage'], catalogs_dir)    
    if db_type in ["ducklake", "dl"]: return ducklakesack.create_db(connection_kwargs['ducklake_catalog'], connection_kwargs['ducklake_storage'], catalogs_dir)
    raise ValueError("unsupported db type")
