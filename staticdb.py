import duckdb
from .models import *

# init vss expressions
SQL_INIT_VSS = "INSTALL vss; LOAD vss;"

# ingest expressions
SQL_READ_PARQUET = lambda filepath: f"""
    CREATE TABLE items AS 
    SELECT * FROM read_parquet('{filepath}');
"""
SQL_READ_JSON = lambda filepath: f"""
    CREATE TABLE items AS 
    SELECT * FROM read_json('{filepath}');
"""
SQL_READ_CSV = lambda filepath: f"""
    CREATE TABLE items AS 
    SELECT * FROM read_csv('{filepath}', header=true);
"""

# query expressions
SQL_WHERE_IN = lambda field, values: field + " IN ( " + (", ".join(f"'{val}'" for val in values)) + " )"
SQL_WHERE_NOT_IN = lambda field, values: field + " NOT IN ( " + (", ".join(f"'{val}'" for val in values)) + " )"

# TODO: make this more generic in future
SQL_VECTOR_SEARCH = lambda embedding: f"""
SELECT 
    {K_ID},
    array_cosine_distance(
        {K_EMBEDDING}::FLOAT[{len(embedding)}], 
        {embedding}::FLOAT[{len(embedding)}]
    ) as distance
FROM items
ORDER BY distance ASC
"""

class StaticDB:    
    db: duckdb.DuckDBPyConnection
    filepath: str

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.db = duckdb.connect()
        # self.db.execute(SQL_INIT_VSS)
        if self.filepath.endswith(".parquet"): self.db.execute(SQL_READ_PARQUET(self.filepath))
        elif self.filepath.endswith(".json"): self.db.execute(SQL_READ_JSON(self.filepath))
        elif self.filepath.endswith(".csv"): self.db.execute(SQL_READ_CSV(self.filepath))
        else: raise ValueError(f"WTF is {filepath}")

    def vector_search(self, embedding: list[float], max_distance: float = 0.0, limit: int = 0) -> list[str]|None:
        conn = self.db.cursor()
        query = conn.sql(SQL_VECTOR_SEARCH(embedding))
        if max_distance: query = query.filter(f"distance <= {max_distance}")
        if limit: query = query.limit(limit)
        return [item[0] for item in query.fetchall()]

