import os
import logging
from pathlib import Path
import pandas as pd

from sqlalchemy import update, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, VARCHAR
from pgvector.sqlalchemy import VECTOR
from sqlalchemy.engine import Engine
from sqlmodel import create_engine
from sqlmodel import Session, SQLModel, Field as SQLField

from .models import *
from .utils import *
from icecream import ic

MAX_CLASSIFICATIONS = 2

# TODO: remove sql alchemy and sqlmodel

class _Bean(Bean, SQLModel, table=True):
    __tablename__ = "beans"
    url: str = SQLField(primary_key=True)    
    embedding: Optional[list[float]] = SQLField(default=None, sa_type=VECTOR(VECTOR_LEN), nullable=True)
    entities: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    regions: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    categories: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    sentiments: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)        

class _Publisher(Publisher, SQLModel, table=True):
    __tablename__ = "publishers"
    source: str = SQLField(primary_key=True)       

class _Chatter(Chatter, SQLModel, table=True):
    __tablename__ = "chatters"
    # NOTE: chatter is not a primary key. this is done to avoid SQLModel bug
    chatter_url: str = SQLField(primary_key=True)
    updated: datetime = SQLField(exclude=True)
    shares: int = SQLField(exclude=True)

class _AggregatedChatter(Chatter, SQLModel, table=True):
    __tablename__ = "_materialized_chatter_aggregates"
    url: str = SQLField(primary_key=True)
    chatter_url: Optional[str] = SQLField(exclude=True)
    collected: Optional[datetime] = SQLField(exclude=True) 

class _TrendingBean(Bean, Chatter, SQLModel, table=True):
    __tablename__ = "trending_beans_view"
    __table_args__ = {'extend_existing': True}
    url: str = SQLField(primary_key=True)    
    embedding: Optional[list[float]] = SQLField(default=None, sa_type=VECTOR(VECTOR_LEN), nullable=True)
    entities: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    regions: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    categories: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    sentiments: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)        
    chatter_url: Optional[str] = SQLField(exclude=True)

class _AggregatedBean(AggregatedBean, SQLModel, table=True):
    __tablename__ = "aggregated_beans_view"
    __table_args__ = {'extend_existing': True}
    url: str = SQLField(primary_key=True)    
    embedding: Optional[list[float]] = SQLField(default=None, sa_type=VECTOR(VECTOR_LEN), nullable=True)
    entities: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    regions: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    categories: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    sentiments: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)        
    related: Optional[list[str]] = SQLField(default=None, sa_type=ARRAY(VARCHAR), nullable=True)
    chatter_url: Optional[str] = SQLField(exclude=True) 
    # tags: Optional[list[str]] = SQLField(exclude=True)   

_TABLES = {
    BEANS: _Bean,
    PUBLISHERS: _Publisher,
    CHATTERS: _Chatter,
    "_materialized_chatter_aggregates": _AggregatedChatter,
    "aggregated_beans_view": _AggregatedBean,
    "trending_beans_view": _TrendingBean   
}
_PRIMARY_KEYS = {
    BEANS: _Bean.url,
    PUBLISHERS: _Publisher.source
}
_PRIMARY_KEY_NAMES = {
    "beans": K_URL,
    "publishers": K_SOURCE
}

ORDER_BY_LATEST = "created DESC"
ORDER_BY_TRENDING = "updated DESC, comments DESC, likes DESC"
ORDER_BY_DISTANCE = "distance ASC"

UPDATE_CLASSIFICATIONS = """
WITH pack AS (
    SELECT 
        b.url,
        ARRAY(
            SELECT category FROM fixed_categories fc
            ORDER BY b.embedding <=> fc.embedding LIMIT 2
        )  AS categories,
        ARRAY(
            SELECT sentiment FROM fixed_sentiments fs
            ORDER BY b.embedding <=> fs.embedding LIMIT 2
        )  AS sentiments
    FROM beans b
    WHERE b.embedding is NOT NULL AND b.categories IS NULL
)
UPDATE beans b
SET 
    categories = pack.categories,
    sentiments = pack.sentiments
FROM pack
WHERE b.url = pack.url;
"""
REFRESH_VIEWS = """
REFRESH MATERIALIZED VIEW CONCURRENTLY _materialized_chatter_aggregates;
REFRESH MATERIALIZED VIEW _materialized_clusters;
REFRESH MATERIALIZED VIEW _materialized_cluster_aggregates;
"""

class Beansack:
    db: Engine

    def __init__(self, conn_str: str):
        """Initialize the Beansack with a PostgreSQL connection string."""
        self.db = create_engine(conn_str)
    
    # STORE METHODS

    def store_beans(self, beans: list[Bean]):
        """Store a list of Beans in the database."""
        if not beans: return 0
        to_store = prepare_beans_for_store(beans)
        stmt = insert(_Bean).values([bean.model_dump() for bean in to_store]).on_conflict_do_nothing(index_elements=[K_URL])
        return self._execute(stmt).rowcount
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        """Partially update a list of Beans in the database."""
        if not beans: return 0
        updates = distinct(beans, K_URL)     
        updates = [bean.model_dump(include=set(columns) | {K_URL}) if columns else bean.model_dump() for bean in updates]
        return self._execute(update(_Bean), updates)
    
    def update_embeddings(self, beans: list[Bean]):
        """Update embeddings for a list of Beans and the computed categories + sentiments during the process."""
        if not beans: return 0
        updates = distinct(beans, K_URL)
        updates = [bean.model_dump(include=[K_URL, K_EMBEDDING]) for bean in updates]
        with Session(self.db) as session:
            session.exec(update(_Bean), params=updates)
            count = session.exec(text(UPDATE_CLASSIFICATIONS)).rowcount
            session.commit()
        return count
    
    def store_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        if not publishers: return 0
        to_store = prepare_publishers_for_store(publishers)
        stmt = insert(_Publisher).values([publisher.model_dump() for publisher in to_store]).on_conflict_do_nothing(index_elements=[K_SOURCE])
        return self._execute(stmt).rowcount
    
    def update_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        if not publishers: return 0
        updates = distinct(publishers, K_SOURCE)
        updates = [publisher.model_dump(exclude=[K_BASE_URL]) for publisher in updates]        
        return self._execute(update(_Publisher), updates)
    
    def store_chatters(self, chatters: list[Chatter]):
        """Store a list of Chatters in the database."""
        if not chatters: return 0
        to_store = prepare_chatters_for_store(chatters)
        stmt = insert(_Chatter).values([chatter.model_dump(exclude=[K_SHARES, K_UPDATED]) for chatter in to_store])
        return self._execute(stmt).rowcount
    
    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items
        get_id = lambda item: getattr(item, _PRIMARY_KEY_NAMES[table])
        existing_ids = ic(self._exists(table, [get_id(item) for item in items]) or [])
        return list(filter(lambda item: get_id(item) not in existing_ids, items))

    def _exists(self, table_name: str, ids: list) -> list:
        if not ids: return
        stmt = select(_PRIMARY_KEYS[table_name]).where(_PRIMARY_KEYS[table_name].in_(ids))
        with Session(self.db) as session:
            result = session.scalars(stmt).all()
        return result
    
    # QUERY METHODS
    def _query_beans(self,
        table: str = BEANS,
        urls: list[str] = None,
        kind: str = None, 
        created: datetime = None, collected: datetime = None, updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order: str = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        model = _TABLES[table]
        if columns: stmt = select(*[getattr(model, col) for col in columns])
        else: stmt = select(model)
        where_clauses = _where(model, 
            urls=urls,
            kind=kind,
            created=created,
            collected=collected,
            updated=updated,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions
        )
        if where_clauses: stmt = stmt.where(*where_clauses)
        if order: stmt = stmt.order_by(text(order))
        if offset: stmt = stmt.offset(offset)
        if limit: stmt = stmt.limit(limit)
        with Session(self.db) as session:
            results = session.exec(stmt).all()
            if columns:
                if len(columns) == 1: results = [model(**{columns[0]: row}) for row in results]
                else: results = [model(**dict(zip(columns, row))) for row in results]
            beans = [Bean.model_validate(row) for row in results]
        return beans

    def query_latest_beans(self,
        kind: str = None, 
        created: datetime = None, collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        return self._query_beans(
            table=BEANS, 
            urls=None,
            kind=kind,
            created=created,
            collected=collected,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_LATEST,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_trending_beans(self,
        kind: str = None, 
        updated: datetime = None, collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        return self._query_beans(
            table="trending_beans_view", 
            urls=None,
            kind=kind,
            updated=updated,
            collected=collected,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_aggregated_beans(self,
        kind: str = None, 
        created: datetime = None, 
        collected: datetime = None,
        updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        return self._query_beans(
            table="aggregated_beans_view",
            kind=kind,
            created=created,
            collected=collected,
            updated=updated,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_LATEST,
            limit=limit,
            offset=offset,
            columns=columns
        )

    def query_aggregated_chatters(self, updated: datetime, limit: int = None):        
        raise NotImplementedError("PostgreSQL implementation of query_aggregated_chatters is not yet available.")

    def query_publishers(self, sources: list[str] = None, conditions: list[str] = None, limit: int = 0):
        stmt = select(_Publisher)
        where_clauses = _where(_Publisher, sources=sources, conditions=conditions)
        if where_clauses: stmt = stmt.where(*where_clauses)
        if limit: stmt = stmt.limit(limit)
        with Session(self.db) as session:
            results = ic(session.exec(stmt).all())
            pubs = [Publisher.model_validate(row) for row in results]
        return pubs

    def count_rows(self, table: str) -> int:
        SQL_COUNT = f"SELECT count(*) FROM {table};"
        with Session(self.db) as session:
            count = session.scalar(text(SQL_COUNT))
        return count
    
    # MAINTENANCE METHODS
    def refresh_classifications(self):        
        return self._execute(text(UPDATE_CLASSIFICATIONS)).rowcount
        
    def refresh(self):
        with Session(self.db) as session:
            result = ic(session.exec(text(UPDATE_CLASSIFICATIONS)).rowcount)
            result = session.exec(text(REFRESH_VIEWS))
            session.commit()
        return result        

    def _execute(self, stmt, params = None):
        with Session(self.db) as session:
            result = session.exec(stmt, params=params)
            session.commit()
        return result
    
    def close(self):
        self.db.dispose()
        
def create_db(conn_str: str, factory_dir: str) -> Beansack:
    """Create the new tables, views, indexes etc."""
    db = Beansack(conn_str)  # Just to ensure the DB is reachable
    with open(os.path.join(os.path.dirname(__file__), 'pgsack.sql'), 'r') as sql_file:
        init_sql = sql_file.read().format(vector_len = VECTOR_LEN, cluster_eps=CLUSTER_EPS)
    db._execute(text(init_sql))

    factory_path = Path(factory_dir) 
    to_list = lambda x: x.tolist() if hasattr(x, 'tolist') else x
    categories = pd.read_parquet(factory_path / "categories.parquet")
    categories[K_EMBEDDING] = categories[K_EMBEDDING].apply(to_list)
    categories.to_sql("fixed_categories", con=db.db, if_exists="replace", index=False, dtype={"category": VARCHAR, K_EMBEDDING: VECTOR(VECTOR_LEN)})
    sentiments = pd.read_parquet(factory_path / "sentiments.parquet")
    sentiments[K_EMBEDDING] = sentiments[K_EMBEDDING].apply(to_list)
    sentiments.to_sql("fixed_sentiments", con=db.db, if_exists="replace", index=False, dtype={"sentiment": VARCHAR, K_EMBEDDING: VECTOR(VECTOR_LEN)})
    return db

def _where(
    model,
    urls: list[str] = None,
    kind: str = None, 
    created: datetime = None, collected: datetime = None, updated: datetime = None,
    categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, 
    sources: list[str] = None, 
    embedding: list[float] = None, distance: float = 0, 
    conditions: list[str] = None,
):
    where_clauses = []
    if urls: where_clauses.append(model.url.in_(urls))
    if kind: where_clauses.append(model.kind == kind)
    if created: where_clauses.append(model.created >= created)
    if collected: where_clauses.append(model.collected >= collected)
    if updated: where_clauses.append(model.updated >= updated)
    # array overlap operator: &&
    if categories: where_clauses.append(model.categories.op("&&")(categories))
    if regions: where_clauses.append(model.regions.op("&&")(regions))
    if entities: where_clauses.append(model.entities.op("&&")(entities))
    if sources: where_clauses.append(model.source.in_(sources))
    # cosine distance operator: <=>
    if embedding and distance: where_clauses.append(model.embedding.cosine_distance(embedding) <= distance)
    if conditions: where_clauses.extend(map(text, conditions))
    return where_clauses