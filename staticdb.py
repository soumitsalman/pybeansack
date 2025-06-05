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
SQL_READ_PARQUET = lambda filepath: f"""
    INSERT INTO items
    SELECT * FROM read_parquet('{filepath}')
    ON CONFLICT DO NOTHING;
"""
SQL_READ_JSON = lambda filepath: f"""
    INSERT INTO items  
    SELECT * FROM read_json('{filepath}')
    ON CONFLICT DO NOTHING;
"""
SQL_READ_CSV = lambda filepath: f"""
    INSERT INTO items
    SELECT * FROM read_csv('{filepath}', header=true)
    ON CONFLICT DO NOTHING;
"""
SQL_READ_DATA = f"""
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
    {K_ID},
    {DISTANCE_FUNCTIONS[metric]}(
        {K_EMBEDDING}::FLOAT[{len(embedding)}], 
        {embedding}::FLOAT[{len(embedding)}]
    ) as distance
FROM items
ORDER BY distance ASC
"""

class StaticDB:    
    db: duckdb.DuckDBPyConnection
    filepath: str

    def __init__(self, filepath: str = None, json_data: list[dict] = None):
        
        self.db = duckdb.connect()
        self.db.execute(SQL_CREATE_EMPTY_TABLE)
        self.store_items(filepath, json_data)        

    def store_items(self, filepath = None, json_data: list[dict] = None):
        cursor = self.db.cursor()
        if filepath:
            self.filepath = filepath
            if self.filepath.endswith(".parquet"): self.db.execute(SQL_READ_PARQUET(self.filepath))
            elif self.filepath.endswith(".json"): self.db.execute(SQL_READ_JSON(self.filepath))
            elif self.filepath.endswith(".csv"): self.db.execute(SQL_READ_CSV(self.filepath))
            else: raise ValueError(f"WTF is {filepath}")
        elif json_data:
            data = pd.DataFrame().from_dict(json_data, orient="columns")
            cursor.execute(SQL_READ_DATA)

    def vector_search(self, embedding: list[float], max_distance: float = 0.0, limit: int = 0, metric: str ="cos") -> list[str]|None:
        cursor = self.db.cursor()
        query = cursor.sql(SQL_VECTOR_SEARCH(embedding, metric))
        if max_distance: query = query.filter(f"distance <= {max_distance}")
        if limit: query = query.limit(limit)
        # query.show()
        return [item[0] for item in query.fetchall()]

