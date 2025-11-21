import os
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, VARCHAR
from pgvector.sqlalchemy import VECTOR
from sqlmodel import create_engine, update, select, delete, text, func
from sqlmodel import SQLModel, Session, Field as SQLField

from .models import *
from .utils import *
from icecream import ic

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

class _Chatter(Chatter, SQLModel, table=True, primary_key=None):
    __tablename__ = "chatters"
    # chatter is not a primary key. this is done to avoid SQLModel bug
    chatter_url: str = SQLField(primary_key=True)
    updated: datetime = SQLField(exclude=True)
    shares: int = SQLField(exclude=True)

_TABLES = {
    BEANS: _Bean,
    PUBLISHERS: _Publisher,
    CHATTERS: _Chatter,
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
        """Update a list of Beans in the database."""
        if not beans: return 0
        updates = distinct(beans, K_URL)     
        updates = [bean.model_dump(include=set(columns) | {K_URL}) if columns else bean.model_dump() for bean in updates]
        update_fields = columns or list(filter(lambda x: x != K_URL, non_null_fields(updates)))
        stmt = insert(_Bean).values(updates)
        stmt = stmt.on_conflict_do_update(
            index_elements=[K_URL],
            set_={col: stmt.excluded[col] for col in update_fields},
            where=_Bean.url.in_([bean.url for bean in beans])
        )
        return self._execute(stmt).rowcount
    
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
        updates = [publisher.model_dump() for publisher in updates]
        update_fields = non_null_fields(updates)
        update_fields = list(filter(lambda x: x not in [K_SOURCE, K_BASE_URL], update_fields))
        stmt = insert(_Publisher).values(updates)
        stmt = stmt.on_conflict_do_update(
            index_elements=[K_SOURCE],
            set_={col: stmt.excluded[col] for col in update_fields},
            where=_Publisher.source.in_([publisher.source for publisher in publishers])
        )
        return self._execute(stmt).rowcount
    
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
            normalize = lambda row: row if isinstance(row, tuple) else (row,)
            results = [model(**dict(zip(columns, normalize(row)))) for row in results]

        return results

    # QUERY METHODS
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
            updated=None,
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

    def query_publishers(self, sources: list[str] = None, conditions: list[str] = None, limit: int = 0):
        stmt = select(_Publisher)
        where_clauses = _where(_Publisher, sources=sources, conditions=conditions)
        if where_clauses: stmt = stmt.where(*where_clauses)
        if limit: stmt = stmt.limit(limit)
        with Session(self.db) as session:
            results = session.exec(stmt).all()
        return results

    def count_rows(self, table: str) -> int:
        SQL_COUNT = f"SELECT count(*) FROM {table};"
        with Session(self.db) as session:
            count = session.scalar(text(SQL_COUNT))
        return count
        
    def refresh(self):
        SQL_REFRESH = "REFRESH MATERIALIZED VIEW CONCURRENTLY WITH DATA;"
        return self._execute(text(SQL_REFRESH))

    def _query(self, sql: str):
        with Session(self.db) as session:
            results = session.exec(text(sql))
            keys = results.keys()
            items = [dict(zip(keys, row)) for row in results]
        return items

    def _execute(self, stmt):
        with Session(self.db) as session:
            result = session.exec(stmt)
            session.commit()
        return result
    
    def close(self):
        self.db.dispose()
        
def create_db(conn_str: str, factory_dir: str) -> Beansack:
    """Create the new tables, views, indexes etc."""
    db = Beansack(conn_str)  # Just to ensure the DB is reachable
    with open(os.path.join(os.path.dirname(__file__), 'pgsack.sql'), 'r') as sql_file:
        init_sql = sql_file.read().format(vector_len = VECTOR_LEN)
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
    categories: list[str] = None, 
    regions: list[str] = None, entities: list[str] = None, 
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
    if categories: where_clauses.append(func.array_overlap(model.categories, categories))
    if regions: where_clauses.append(func.array_overlap(model.regions, regions))
    if entities: where_clauses.append(func.array_overlap(model.entities, entities))
    if sources: where_clauses.append(model.source.in_(sources))
    if embedding and distance: where_clauses.append(model.embedding.cosine_distance(embedding) < distance)
    if conditions: where_clauses.extend(map(text, conditions))
    return where_clauses