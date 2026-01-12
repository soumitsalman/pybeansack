import duckdb
import pandas as pd
from .models import *

# init vss expressions
SQL_INIT_VSS = "INSTALL vss; LOAD vss;"

# TODO: change the vector length later
SQL_CREATE_EMPTY_TABLE = """
CREATE TABLE IF NOT EXISTS items (
    _id VARCHAR PRIMARY KEY,
    embedding FLOAT[384]
);
"""
# ingest expressions
SQL_CREATE_FROM_PARQUET = lambda filepath: f"""
    CREATE TABLE items AS
    SELECT * FROM read_parquet('{filepath}');
"""
SQL_CREATE_FROM_JSON = lambda filepath: f"""
    CREATE TABLE items AS
    SELECT * FROM read_json('{filepath}');
"""
SQL_CREATE_FROM_CSV = lambda filepath: f"""
    CREATE TABLE items AS
    SELECT * FROM read_csv('{filepath}', header=true);
"""
SQL_CREATE_FROM_PANDAS = f"""
    CREATE TABLE items AS
    SELECT * FROM data;
"""

# ingest expressions
SQL_INSERT_PARQUET = lambda filepath: f"""
    INSERT INTO items
    SELECT * FROM read_parquet('{filepath}')
    ON CONFLICT DO NOTHING;
"""
SQL_INSERT_JSON = lambda filepath: f"""
    INSERT INTO items  
    SELECT * FROM read_json('{filepath}')
    ON CONFLICT DO NOTHING;
"""
SQL_INSERT_CSV = lambda filepath: f"""
    INSERT INTO items
    SELECT * FROM read_csv('{filepath}', header=true)
    ON CONFLICT DO NOTHING;
"""
SQL_INSERT_PANDAS = f"""
    INSERT INTO items 
    SELECT * FROM data 
    ON CONFLICT DO NOTHING;
"""

# query expressions
SQL_WHERE_IN = lambda field, values: field + " IN ( " + (", ".join(f"'{val}'" for val in values)) + " )"
SQL_WHERE_NOT_IN = lambda field, values: field + " NOT IN ( " + (", ".join(f"'{val}'" for val in values)) + " )"

DISTANCE_FUNCTIONS = {
    "cos": "array_cosine_distance",
    "l2": "array_distance",
    "dot": "array_dot_product"
}
# TODO: make this more generic in future
SQL_VECTOR_SEARCH = lambda embedding, metric: f"""
SELECT 
    id,
    {DISTANCE_FUNCTIONS[metric]}(
        embedding::FLOAT[{len(embedding)}], 
        {embedding}::FLOAT[{len(embedding)}]
    ) as distance
FROM items
ORDER BY distance ASC
"""

class StaticDB:    
    db: duckdb.DuckDBPyConnection

    def __init__(self, data: str|list[dict] = None, file_cache: str = None, read_only = False):
        """
        Create a Vector DB with fields _id and embedding.
        If data is a string then it will be treated as a filepath to load data from
        If data is a list[dict] then it will be treated as a list of json objects representing the data
        If nothing is provided an empty table will be created
        """
        if file_cache: self.db = duckdb.connect(file_cache, read_only=read_only)
        else: self.db = duckdb.connect(read_only=read_only)

        if not data: self.db.execute(SQL_CREATE_EMPTY_TABLE)
        elif isinstance(data, str):
            if data.endswith(".parquet"): self.db.execute(SQL_CREATE_FROM_PARQUET(data))
            elif data.endswith(".json"): self.db.execute(SQL_CREATE_FROM_JSON(data))
            elif data.endswith(".csv"): self.db.execute(SQL_CREATE_FROM_CSV(data))
            else: raise NotImplementedError(f"{data} file extention not supported")
        elif isinstance(data, list):
            data = pd.DataFrame().from_dict(data, orient="columns")
            self.db.execute(SQL_CREATE_FROM_PANDAS)
        else: raise ValueError("WTF is this")
        
    def store_items(self, data: str|list[dict]):
        cursor = self.db.cursor()
        if isinstance(data, str):
            if data.endswith(".parquet"): self.db.execute(SQL_INSERT_PARQUET(data))
            elif data.endswith(".json"): self.db.execute(SQL_INSERT_JSON(data))
            elif data.endswith(".csv"): self.db.execute(SQL_INSERT_CSV(data))
            else: raise NotImplementedError(f"{data} file extention not supported")
        elif isinstance(data, list):
            data = pd.DataFrame().from_dict(data, orient="columns")
            cursor.execute(SQL_INSERT_PANDAS)

    def vector_search(self, embedding: list[float], distance: float = 0.0, limit: int = 0, metric: str ="cos") -> list[str]|None:
        cursor = self.db.cursor()
        query = cursor.sql(SQL_VECTOR_SEARCH(embedding, metric))
        if distance: query = query.filter(f"distance <= {distance}")
        if limit: query = query.limit(limit)
        # query.show()
        from icecream import ic
        return [item[0] for item in ic(query.fetchall())]

