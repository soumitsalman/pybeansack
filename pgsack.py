import os
import logging
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

class Beansack:
    db: Engine

    def __init__(self, conn_str: str):
        """Initialize the Beansack with a PostgreSQL connection string."""
        self.db = create_engine(conn_str)
    
    def store_beans(self, beans: list[Bean]):
        """Store a list of Beans in the database."""
        if not beans: return 0
        to_store = prepare_beans_for_store(beans)
        stmt = insert(_Bean).values([bean.model_dump() for bean in to_store]).on_conflict_do_nothing(index_elements=[K_URL])
        return self._execute(stmt).rowcount
    
    def store_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        if not publishers: return 0
        to_store = prepare_publishers_for_store(publishers)
        stmt = insert(_Publisher).values([publisher.model_dump() for publisher in to_store]).on_conflict_do_nothing(index_elements=[K_SOURCE])
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

    def count_rows(self, table: str) -> int:
        SQL_COUNT = f"SELECT count(*) FROM {table};"
        with Session(self.db) as session:
            count = session.scalar(text(SQL_COUNT))
        return count
        
    def refresh(self):
        SQL_REFRESH = "REFRESH MATERIALIZED VIEW CONCURRENTLY WITH DATA;"
        with Session(self.db) as session:
            session.exec(text(SQL_REFRESH))
            session.commit()

    def _execute(self, stmt):
        with Session(self.db) as session:
            result = session.exec(stmt)
            session.commit()
        return result
    
    def close(self):
        self.db.dispose()
        
def create_db(conn_str: str) -> Beansack:
    """Create the new tables, views, indexes etc."""
    db = Beansack(conn_str)  # Just to ensure the DB is reachable
    with open(os.path.join(os.path.dirname(__file__), 'pgsack.sql'), 'r') as sql_file:
        init_sql = sql_file.read().format(vector_len = VECTOR_LEN)
    db._execute(text(init_sql))
    return db