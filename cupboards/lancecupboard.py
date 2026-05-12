# NOTE: this is deprecated. IGNORE

from datetime import datetime
from deprecation import deprecated
import lancedb
from lancedb.pydantic import LanceModel
from pydantic import BaseModel

Sip = BaseModel
_Sip = LanceModel
SIPS = "sips"

@deprecated
class LanceDBCupboard: 
    db: lancedb.DBConnection
    tables: dict[str, lancedb.Table]

    def __init__(self, storage_path: str):
        self.db = _connect(storage_path)
        self.tables = {
            SIPS: self.db.create_table(SIPS, schema=_Sip, exist_ok=True)
        }

    # INGESTION functions   
    def store_sips(self, sips: list[Sip]):
        if not sips: return 0

        result = self.tables[SIPS].merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute([_Sip(**sip.model_dump(exclude_none=True)) for sip in sips])
        return result.num_inserted_rows

    def count_rows(self, table=SIPS, conditions: list[str] = None) -> int:
        where_exprs = _where(conditions=conditions)
        return self.tables[table].count_rows(where_exprs)

    def _query_items(self,
        table: str,
        created: datetime = None, updated: datetime = None,
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        query = self.tables[table].search() if not embedding else self.tables[table].search(query=embedding, query_type="vector", vector_column_name=K_EMBEDDING)      
        where_expr = _where(created=created, updated=updated, conditions=conditions)
        if where_expr: query = query.where(where_expr)
        if embedding: query = query.distance_type("cosine")
        if distance: query = query.distance_range(upper_bound = distance)
        if order and embedding: query = query.rerank(order, query_string="default")
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns+(["_distance"] if embedding else []))
        return query.to_pydantic(_Sip)
    
    def _query_with_multiple_vectors(self,
        table: str,
        created: datetime = None, updated: datetime = None,
        vectors: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Sip]:
        query = self.tables[table].search() if not vectors else self.tables[table].search(query=vectors, query_type="vector", vector_column_name=K_EMBEDDING)      
        where_expr = _where(created=created, updated=updated, conditions=conditions)
        if where_expr: query = query.where(where_expr)
        if vectors: query = query.distance_type("cosine")
        if distance: query = query.distance_range(upper_bound = distance)
        if order and vectors: query = query.rerank(order, query_string="default")
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns+(["_distance"] if vectors else []))
        
        # Get all rows and deduplicate by 'id'
        df = query.to_pandas().drop_duplicates(subset=['id'], keep='first')
        return [_Sip(**sip) for sip in df.to_dict('records')]
    
    def query_sips(self,
        created: datetime = None, updated: datetime = None,
        embedding: list[float]|list[list[float]] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Sip]:
        if embedding and isinstance(embedding[0], list):
            return self._query_with_multiple_vectors(
                table=SIPS,
                created=created,
                updated=updated,
                vectors=embedding,
                distance=distance,
                conditions=conditions,
                limit=limit,
                offset=offset,
                columns=columns
            )
        return self._query_items(
            table=SIPS,
            created=created,
            updated=updated,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def _remove_from(self, table: str, created: datetime = None, conditions: list[str] = None) -> int:       
        where_expr = _where(created=created, conditions=conditions)
        current_total = self.tables[table].count_rows()
        self.tables[table].delete(where_expr)
        return current_total - self.tables[table].count_rows()
    
    def remove_sips(self, created: datetime = None, conditions: list[str] = None) -> int:
        return self._remove_from(SIPS, created=created, conditions=conditions)
    
    def optimize(self):
        # NOTE: something wrong with the vector index creation
        # try: [self.tables[table].create_index(vector_column_name=K_EMBEDDING, index_type="IVF_PQ", metric="cosine") for table in [MUGS, SIPS]]
        # except: pass
        [table.optimize() for table in self.tables.values()]

    def close(self):
        del self.db
        del self.tables
