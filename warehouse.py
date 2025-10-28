from dataclasses import fields
import os
import random
import logging
import duckdb
from duckdb import TransactionException
import pandas as pd
from datetime import datetime
from retry import retry
from .models import *
from .utils import *
from icecream import ic

RETRY_COUNT = 10
RETRY_DELAY = (1,5)  # seconds

DIGEST_COLUMNS = [K_URL, K_GIST, K_CATEGORIES, K_SENTIMENTS]

log = logging.getLogger(__name__)

def _select(table: str, columns: list[str] = None, embedding: list[float] = None):
    if columns: fields = columns.copy()
    else: fields = ["*"]
    if embedding: fields.append(f"array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance")
    return f"SELECT {', '.join(fields)} FROM warehouse.{table}", [embedding] if embedding else None

def _where(
    kind: str = None,
    created: datetime = None,
    collected: datetime = None,
    updated: datetime = None,
    categories: list[str] = None,
    regions: list[str] = None,
    entities: list[str] = None,
    sources: list[str] = None,  
    distance: float = 0,
    exprs: list[str] = None
):
    conditions = []
    params = []
    if kind: conditions.append("kind = ?"), params.append(kind)
    if created: conditions.append("created >= ?"), params.append(created)
    if collected: conditions.append("collected >= ?"), params.append(collected)
    if updated: conditions.append("updated >= ?"), params.append(updated)
    if categories: conditions.append("ARRAY_HAS_ANY(categories, ?)"), params.append(categories)
    if regions: conditions.append("ARRAY_HAS_ANY(regions, ?)"), params.append(regions)
    if entities: conditions.append("ARRAY_HAS_ANY(entities, ?)"), params.append(entities)
    if sources: conditions.append(f"source IN ({', '.join('?' for _ in sources)})"), params.extend(sources)
    if distance: conditions.append("distance <= ?"), params.append(distance)
    if exprs: conditions.extend(exprs)

    if conditions: return " WHERE "+ (" AND ".join(conditions)), params
    return None, None

_deduplicate =lambda items, key: list({getattr(item, key): item for item in items}.values())  # deduplicate by url

class Beansack:
    def __init__(self, catalogdb: str, storagedb: str, factory_dir: str = "factory"):
        config = {
            'threads': max(os.cpu_count() >> 1, 1),
            'enable_http_metadata_cache': True,
            'ducklake_max_retry_count': 100
        }
        
        with open(os.path.join(os.path.dirname(__file__), 'warehouse.sql'), 'r') as sql_file:
            init_sql = sql_file.read().format(
                # loading prefixed categories and sentiments
                factory=os.path.expanduser(factory_dir),
                catalog_path= f"postgres:{catalogdb}" if catalogdb.startswith("postgresql://") else catalogdb,
                data_path=os.path.expanduser(storagedb),
            )

        self.db = duckdb.connect(config=config) 
        self.execute(init_sql)
        log.debug("Data warehouse initialized.")
    
    def _exists(self, table: str, field: str, ids: list) -> list[str]:
        if not ids: return
        SQL_EXISTS = f"SELECT {field} FROM warehouse.{table} WHERE {field} IN ({','.join('?' for _ in ids)})"
        return self.query(SQL_EXISTS, params=ids)

    ##### Store methods
    def _beans_to_df(self, beans: list[Bean], columns):
        EXCLUDE_COLUMNS = ["tags", "chatter", "publisher", "trend_score", "updated", "distance"]
        beans = _deduplicate(beans, K_URL)
        if columns: beans = [bean.model_dump(include=columns) for bean in beans] 
        else: beans = [bean.model_dump(exclude_none=True, exclude=EXCLUDE_COLUMNS) for bean in beans] 
        
        df = pd.DataFrame(beans)
        fields = columns or [col for col in df.columns if df[col].notnull().any()]
        dtype_specs = {field:mapping for field, mapping in Bean.Config.dtype_specs.items() if field in [fields]}
        return df.astype(dtype_specs)

    @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _execute_df(self, sql, df):
        cursor = self.db.cursor()
        cursor.execute(sql)        
        cursor.close()
        return True

    def store_beans(self, beans: list[Bean]) -> list[Bean]:                   
        if not beans: return
       
        bean_filter = lambda bean: bool(bean.title and bean.collected and bean.created and bean.source and bean.kind)
        df = self._beans_to_df(list(filter(bean_filter, rectify_bean_fields(beans))), None)
        # insert the non null columns
        fields=', '.join(df.columns)
        SQL_INSERT = f"""
        INSERT INTO warehouse.beans ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.beans b
            WHERE b.url = df.url
        );
        """
        return self._execute_df(SQL_INSERT, df)
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return None

        df = self._beans_to_df(beans, list(set(columns+[K_URL])) if columns else None)
        fields = columns or df.columns
        updates = [f"{f} = pack.{f}" for f in fields]
        SQL_UPDATE = f"""
        MERGE INTO warehouse.beans
        USING (SELECT url, {', '.join(fields)} FROM df) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET {', '.join(updates)};
        """
        return self._execute_df(SQL_UPDATE, df)    

    def refresh_classifications(self):
        SQL_UPDATE_CLASSIFICATION = f"""
        WITH needs_classification AS (
            SELECT url, embedding FROM warehouse.beans
            WHERE categories IS NULL AND embedding IS NOT NULL
        )
        MERGE INTO warehouse.beans
        USING (
            SELECT url, LIST(category) as categories, LIST(sentiment) as sentiments FROM needs_classification nc
            LEFT JOIN LATERAL (
                SELECT category FROM warehouse.fixed_categories
                ORDER BY array_cosine_distance(nc.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 3
            ) ON TRUE
            LEFT JOIN LATERAL (
                SELECT sentiment FROM warehouse.fixed_sentiments
                ORDER BY array_cosine_distance(nc.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 3
            ) ON TRUE
            GROUP BY url
        ) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET categories = pack.categories, sentiments = pack.sentiments;
        """
        return self.execute(SQL_UPDATE_CLASSIFICATION)
    
    def refresh_related_beans(self):
        # # NOTE: the current timestamp is necessary for both queries
        # # otherwise it will loop for ever
        # SQL_COUNT_MISSING_CLUSTERS = """
        # SELECT count(*) FROM warehouse.beans 
        # WHERE 
        #     cluster_id IS NULL 
        #     AND embedding IS NOT NULL
        #     AND collected >= CURRENT_TIMESTAMP - INTERVAL '28 days';
        # """

        # calculate and update for a batch of 512, otherwise it dies
        # first insert into related beans
        SQL_INSERT_RELATED = f"""
        WITH 
            scope AS (
                SELECT url, embedding FROM warehouse.beans                 
                WHERE embedding IS NOT NULL
            ),
            needs_relating AS (
                SELECT s.* FROM scope s
                INNER JOIN warehouse._internal_related_beans r ON s.url = r.url
                WHERE r.related IS NULL         
            )
        INSERT INTO warehouse._internal_related_beans        
        SELECT nr.url as url, s.url as related, abs(array_distance(nr.embedding::FLOAT[{VECTOR_LEN}], s.embedding::FLOAT[{VECTOR_LEN}])) as distance 
        FROM needS_relating nr
        CROSS JOIN scope s
        WHERE distance <= {CLUSTER_EPS};
        """
        return self.execute(SQL_INSERT_RELATED)
    
    def refresh_clusters(self):
        SQL_UPDATE_CLUSTERS = f"""
        WITH 
            needs_clustering AS (
                SELECT rb.* FROM warehouse._internal_related_beans rb
                INNER JOIN warehouse.beans b ON rb.url = b.url
                WHERE b.cluster_id IS NULL
            ),
            cluster_sizes AS (
                SELECT related, count(*) AS cluster_size 
                FROM warehouse._internal_related_beans 
                GROUP BY related
            )
        MERGE INTO warehouse.beans
        USING (
            SELECT 
                url, 
                FIRST(cl.related ORDER BY cluster_size DESC) AS cluster_id,
                COUNT(*) AS cluster_size
            FROM needs_clustering cl
            INNER JOIN cluster_sizes clsz ON cl.related = clsz.related
            GROUP BY url
        ) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET cluster_id = pack.cluster_id, cluster_size = pack.cluster_size;"""
        return self.execute(SQL_UPDATE_CLUSTERS)  

    # def _store_related_beans(self, relations: list[dict]):
    #     if not relations: return

    #     df = pd.DataFrame(relations)
    #     fields=', '.join(col for col in df.columns if df[col].notnull().any())
    #     SQL_INSERT = f"""
    #     INSERT INTO warehouse._internal_related_beans ({fields})
    #     SELECT {fields} FROM df
    #     WHERE NOT EXISTS (
    #         SELECT 1 FROM warehouse._internal_related_beans r
    #         WHERE r.url = df.url AND r.related = df.related
    #     );
    #     """
    #     self._execute_df(SQL_INSERT, df)     
        # return self._update_clusters()

    def store_chatters(self, chatters: list[Chatter]):       
        if not chatters: return

        chatter_filter = lambda x: bool(x.chatter_url and x.url and (x.likes or x.comments or x.subscribers))
        chatters = list(filter(chatter_filter, chatters))
        if not chatters: return

        df = pd.DataFrame([chatters.model_dump(exclude=[K_SHARES, K_UPDATED]) for chatters in chatters])        
        fields=', '.join(col for col in df.columns if df[col].notnull().any())
        SQL_INSERT = f"""
        INSERT INTO warehouse.chatters ({fields})
        SELECT {fields} FROM df;
        """
        return self._execute_df(SQL_INSERT, df)     
    
    def refresh_chatter_aggregates(self):  
        # WHERE updated >= CURRENT_TIMESTAMP - INTERVAL '1 month';  
        SQL_INSERT_AGGREGATES = f"""
        INSERT INTO warehouse._internal_chatter_aggregates        
        SELECT *, CURRENT_TIMESTAMP as refresh_ts 
        FROM warehouse._internal_chatter_aggregates_view;        

        DELETE FROM warehouse._internal_chatter_aggregates    
        WHERE refresh_ts < CURRENT_TIMESTAMP - INTERVAL '1 hour';
        """
        return self.execute(SQL_INSERT_AGGREGATES)  

    def store_publishers(self, publishers: list[Publisher]): 
        if not publishers: return

        pub_filter = lambda x: bool(x.source and x.base_url)
        publishers = list(filter(pub_filter, _deduplicate(publishers, K_SOURCE)))
        if not publishers: return
        
        df = pd.DataFrame([pub.model_dump(exclude_none=True) for pub in publishers])
        fields=', '.join(col for col in df.columns if df[col].notnull().any())
        SQL_INSERT = f"""
        INSERT INTO warehouse.publishers ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.publishers p
            WHERE p.source = df.source
        );
        """
        return self._execute_df(SQL_INSERT, df)

    ###### Query methods
    def deduplicate(self, table: str, idkey: str, items: list) -> list:
        if not items: return items
        ids = [getattr(item, idkey) for item in items]
        existing_ids = self._exists(table, idkey, ids) or []
        return [item for id, item in zip(ids, items) if id not in existing_ids]
    
    def exists(self, urls: list[str]) -> list[str]:
        return self._exists("bean", "url", urls)
    
    def count_items(self, table):
        SQL_COUNT = f"SELECT count(*) FROM warehouse.{table};"
        return self.query_one(SQL_COUNT)

    def _query_beans(self, 
        table: str = "beans",
        kind: str = None, 
        created: datetime = None, collected: datetime = None, updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        exprs: list[str] = None,
        order: str = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        expr, select_params = _select(table, columns, embedding)
        where_expr, where_params = _where(kind, created, collected, updated, categories, regions, entities, sources, distance, exprs)
        if where_expr: expr += where_expr
        params = []
        if select_params: params.extend(select_params)
        if where_params: params.extend(where_params)

        cursor = self.db.cursor()
        rel = cursor.query(expr, params=params)
        if order: rel = rel.order(order)
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        beans = [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
        cursor.close()

        return beans
    
    def query_latest_beans(self,
        kind: str = None, 
        created: datetime = None, 
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        exprs: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        if columns: fields = list(set(columns + [K_CREATED]))
        else: fields = None
        return self._query_beans(
            table="beans",
            kind=kind,
            created=created,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            exprs=exprs,
            order="created DESC",
            limit=limit,
            offset=offset,
            columns=fields
        )
    
    def query_trending_beans(self,
        kind: str = None, 
        updated: datetime = None, 
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        exprs: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        if columns: fields = list(set(columns + [K_UPDATED, K_COMMENTS, K_LIKES]))
        else: fields = None
        return self._query_beans(
            table="trending_beans_view",
            kind=kind,
            updated=updated,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            exprs=exprs,
            order="updated DESC, comments DESC, likes DESC",
            limit=limit,
            offset=offset,
            columns=fields
        )

    def query_aggregated_chatters(self, updated: datetime, limit: int = None):        
        SQL_LATEST_CHATTERS = """
        SELECT a.* FROM warehouse._internal_chatter_aggregates a
        JOIN (
            SELECT url, MAX(refresh_ts) AS max_refresh
            FROM warehouse._internal_chatter_aggregates
            WHERE updated >= ?
            GROUP BY url
        ) mx ON a.url = mx.url AND a.refresh_ts = mx.max_refresh;
        """
        cursor = self.db.cursor()
        rel = cursor.query(SQL_LATEST_CHATTERS, params=(updated,))
        if limit: rel = rel.limit(limit)
        return [Chatter(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
    def query_publishers(self, conditions: list[str] = None, limit: int = None):        
        query_expr = "SELECT * FROM warehouse.publishers"
        if conditions: query_expr = query_expr + " WHERE " + " AND ".join(conditions)
        cursor = self.db.cursor()
        rel = cursor.query(query_expr)
        if limit: rel = rel.limit(limit)
        return [Publisher(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
    ##### Maintenance methods
    def query(self, query_expr: str, params: list = None) -> list[dict]:
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        return [dict(zip(rel.columns, row)) for row in rel.fetchall()]
    
    def query_one(self, query_expr: str, params: list = None):
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        count = rel.fetchone()[0]
        cursor.close()
        return count

    @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def execute(self, expr: str, params: list = None):
        cursor = self.db.cursor()
        cursor.execute(expr, params)
        cursor.close()

    def recompute(self):    
        ic(self.refresh_classifications())
        log.debug("Refreshed classifications.")
        ic(self.refresh_related_beans())
        log.debug("Refreshed related beans.")
        ic(self.refresh_clusters())
        log.debug("Refreshed clusters.")
        ic(self.refresh_chatter_aggregates())
        log.debug("Refreshed chatter aggregates.")

    def cleanup(self):
        SQL_CLEANUP = """
        -- CALL ducklake_merge_adjacent_files('warehouse');
        CALL ducklake_expire_snapshots('warehouse', older_than => now() - INTERVAL '1 day');
        CALL ducklake_cleanup_old_files('warehouse', cleanup_all => true);
        CALL ducklake_delete_orphaned_files('warehouse', cleanup_all => true);
        """     
        self.db.execute(SQL_CLEANUP)
        log.debug("Compacted data files.")

    def snapshot(self):
        SQL_CURRENT_SNAPSHOT = "SELECT * FROM warehouse.current_snapshot();"
        return self.query_one(SQL_CURRENT_SNAPSHOT)
    
    def close(self):
        if not self.db: return        
        self.db.close()        
        log.debug("Database connection closed.")

    # TODO: backup catalog somewhere
    def backup(self):
        raise NotImplementedError("Backup not implemented")

 