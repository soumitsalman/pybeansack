import os
from pydantic import Field
import lancedb
from lancedb.pydantic import LanceModel, Vector
from datetime import datetime, timedelta
import pyarrow as pa
import pandas as pd
from .models import *
from icecream import ic

class _Bean(Bean, LanceModel):
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None)

class _Publisher(Publisher, LanceModel):
    pass

class _Chatter(Chatter, LanceModel):
    pass

class _Sip(Sip, LanceModel):    
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None, description="This is the embedding vector of title+content")
    
class _Mug(Mug, LanceModel):    
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None, description="This is the embedding vector of title+content")

def _connect(storage_path: str):
    storage_options = None
    if storage_path.startswith("s3://"):
        storage_options = {
            "access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
            "endpoint": os.getenv("S3_ENDPOINT"),
            "region": os.getenv("S3_REGION"),
            "timeout": "60s"
        }
    return lancedb.connect(
        uri=storage_path, 
        read_consistency_interval = timedelta(hours=1),
        storage_options=storage_options
    )

def establish_db(storage_path: str, factory_dir: str):
    db = _connect(storage_path)

    db.create_table("beans", schema=_Bean, exist_ok=True)
    db.create_table("publishers", schema=_Publisher, exist_ok=True)
    db.create_table("chatters", schema=_Chatter, exist_ok=True)
    db.create_table("mugs", schema=_Mug, exist_ok=True)
    db.create_table("sips", schema=_Sip, exist_ok=True)
    db.create_table(
        "fixed_categories", 
        pd.read_parquet(f"{factory_dir}/categories.parquet"),
        mode="overwrite"
    )
    db.create_table(
        "fixed_sentiments", 
        pd.read_parquet(f"{factory_dir}/sentiments.parquet"),
        mode="overwrite"
    )
    # TODO: put indexes on url, source and embedding (vector)

    return db

class Beansack: 
    db: lancedb.DBConnection
    allmugs: lancedb.Table
    allsips: lancedb.Table
    allbeans: lancedb.Table
    allpublishers: lancedb.Table
    allchatters: lancedb.Table

    def __init__(self, storage_path: str):
        self.db = _connect(storage_path)
        self.allbeans = self.db.open_table("beans")        
        self.allpublishers = self.db.open_table("publishers")
        self.allchatters = self.db.open_table("chatters")
        self.allmugs = self.db.open_table("mugs")
        self.allsips = self.db.open_table("sips")
        self.fixed_categories = self.db.open_table("fixed_categories")
        self.fixed_sentiments = self.db.open_table("fixed_sentiments")

    def store_beans(self, beans: list[Bean]):
        if not beans: return 0

        to_store = rectify_bean_fields(beans)
        to_store = list(filter(bean_filter, to_store))
        to_store = distinct(to_store, "url")        
        result = self.allbeans.merge_insert("url") \
            .when_not_matched_insert_all() \
            .execute([_Bean(**bean.model_dump(exclude_none=True)) for bean in to_store])
        return result.num_inserted_rows
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return 0

        if columns:
            fields = list(set(columns + [K_URL]))
            updates = [bean.model_dump(include=fields) for bean in beans]
        else:
            updates = [bean.model_dump(exclude_none=True) for bean in beans]
            fields = non_null_fields(updates)

        get_field_values = lambda field: [update.get(field) for update in updates]
        result = self.allbeans.merge_insert("url") \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data={field: get_field_values(field) for field in fields},
                    schema=pa.schema(list(map(self.allbeans.schema.field, fields)))
                )
            )
        return result.num_updated_rows
    
    # this assuming that the embedding field is already set
    # this is a specialized update that also updates categories, sentiments and clusters
    def update_embeddings(self, beans: list[Bean]):
        if not beans: return 0

        vecs = [bean.embedding for bean in beans]
        categories = self.fixed_categories.search(vecs).distance_type("cosine").limit(3).select(["category"]).to_pandas()
        sentiments = self.fixed_sentiments.search(vecs).distance_type("cosine").limit(3).select(["sentiment"]).to_pandas()
        updates = {
            K_URL: [bean.url for bean in beans],
            K_EMBEDDING: vecs,
            K_CATEGORIES: categories.groupby('query_index')['category'].apply(list).sort_index().tolist(),
            K_SENTIMENTS: sentiments.groupby('query_index')['sentiment'].apply(list).sort_index().tolist()
        } 
        result = self.allbeans.merge_insert("url") \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data=updates,
                    schema=pa.schema(
                        list(map(self.allbeans.schema.field, [K_URL, K_EMBEDDING, K_CATEGORIES, K_SENTIMENTS]))
                    )
                )
            )
        embs_updated = result.num_updated_rows
        # TODO: update clusters
        self.allbeans.search(vecs).distance_type("l2").distance_range(upper_bound=CLUSTER_EPS).select(["url"]).to_list()
        # --- IGNORE ---
        return embs_updated

    def store_publishers(self, publishers: list[Publisher]):
        if not publishers: return 0

        to_store = rectify_publisher_fields(publishers)        
        to_store = list(filter(publisher_filter, to_store))
        to_store = distinct(to_store, K_SOURCE)
        result = self.allpublishers.merge_insert("source") \
            .when_not_matched_insert_all() \
            .execute([_Publisher(**publisher.model_dump(exclude_none=True)) for publisher in to_store])
        return result.num_inserted_rows

    def store_chatters(self, chatters: list[Chatter]):
        if not chatters: return 0

        to_store = rectify_chatter_fields(chatters)
        to_store = list(filter(chatter_filter, to_store))
        self.allchatters.add([_Chatter(**chatter.model_dump(exclude_none=True, exclude=[K_SHARES, K_UPDATED])) for chatter in to_store])
        return len(to_store)

    def store_mugs(self, mugs: list[Mug]):
        if not mugs: return 0

        to_store = distinct(mugs, "id")
        result = self.allmugs.merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute( [_Mug(**mug.model_dump(exclude_none=True)) for mug in to_store])
        return result.num_inserted_rows
    
    def store_sips(self, sips: list[Sip]):
        if not sips: return 0

        to_store = distinct(sips, "id") 
        result = self.allsips.merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute([_Sip(**sip.model_dump(exclude_none=True)) for sip in to_store])
        return result.num_inserted_rows