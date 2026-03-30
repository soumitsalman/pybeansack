
from datetime import datetime, timezone

import lancedb
from lancedb.pydantic import LanceModel, Vector
import pyarrow as pa
from typing import Any, Literal, Optional
from .utils import VECTOR_LEN

ITEMS = "__items__"
ID = "id"
TS = "ts"
INDEXING_THRESHOLD = 100000
DISTANCE_FUNC = Literal["l2", "cosine", "dot"]

class SimpleVectorDB:
    db: lancedb.DBConnection
    id_keys: dict[str, str]
    tables: dict[str, lancedb.Table]

    def __init__(self, db_path: str, table_id_keys: dict[str, str]):
        self.db = lancedb.connect(db_path)
        self.id_keys = table_id_keys.copy()
        self.tables = {name: self.db.open_table(name) for name in self.db.table_names()}
        
    @classmethod
    def create_db(cls, db_path: str, table_id_keys: dict[str, str], **additional_tables):
        """Classifications should be dict[str, pd.DataFrame|list[dict[str, list[float]]]] where the dataframe has columns "category" or "sentiment" and "embedding" """
        db = lancedb.connect(db_path)
        # setup main tables
        for table, id_key in table_id_keys.items():
            db.create_table(
                table, 
                schema=pa.schema([
                    (id_key, pa.string()), 
                    ("embedding", pa.list_(pa.float32(), VECTOR_LEN)),
                    (TS, pa.timestamp("s", tz="UTC"))
                ]), 
                mode="overwrite"
            ).create_scalar_index(id_key, replace=True)
        # setup additonal tables
        for cls_name, cls_values in additional_tables.items():
            db.create_table(cls_name, data=cls_values, mode="overwrite")
        return cls(db_path, table_id_keys=table_id_keys)

    def store(self, table: str, items: list[dict[str, Any]]):
        if not items: return 0

        id_key = self.id_keys[table]
        from icecream import ic
        result = self.tables[table].merge_insert(id_key) \
            .when_not_matched_insert_all() \
                .execute(_prepare_to_store(items, id_key))
        return result.num_inserted_rows
        
    def search(self, table: str, embedding: list[float], distance_func: DISTANCE_FUNC = "l2", distance: Optional[float] = None, limit: Optional[int] = None, columns: list[str] = None):
        query = self.tables[table].search(embedding, vector_column_name="embedding", query_type="vector").distance_type(distance_func)  
        if distance: query = query.distance_range(upper_bound = distance)
        if limit: query = query.limit(limit)
        if columns: query = query.select(columns+["_distance"])
        return query.to_list()
    
    def optimize(self, **kwargs):
        # TODO: put vector indexing on ITEMS after it cross a certain threshold
        [tbl.optimize(**kwargs) for tbl in self.tables.values()]

    def close(self):
        self.optimize()
        del self.db
        del self.tables

def _prepare_to_store(items: list[dict[str, Any]], id_key: str):
    """attaches timestamps"""
    ts = int(datetime.now(tz=timezone.utc).timestamp())
    for item in items:
        item[TS] = ts        
    return items
