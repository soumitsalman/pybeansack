import os
from pydantic import Field
import lancedb
from lancedb.pydantic import LanceModel, Vector
from datetime import datetime, timedelta
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
  
class Beansack: 
    db = None
    allmugs = None
    allsips = None
    allbeans = None
    allpublishers = None
    allchatters = None

    def __init__(self, storage_path: str):
        storage_options = None
        if storage_path.startswith("s3://"):
            storage_options = {
                "access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
                "secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
                "endpoint": os.getenv("S3_ENDPOINT"),
                "region": os.getenv("S3_REGION"),
                "timeout": "60s"
            }
        self.db = lancedb.connect(
            uri=storage_path, 
            read_consistency_interval = timedelta(hours=1),
            storage_options=storage_options
        )
       
        self.allbeans = self.db.create_table("beans", schema=_Bean, exist_ok=True)        
        self.allpublishers = self.db.create_table("publishers", schema=_Publisher, exist_ok=True)
        self.allchatters = self.db.create_table("chatters", schema=_Chatter, exist_ok=True)
        self.allmugs = self.db.create_table("mugs", schema=_Mug, exist_ok=True)
        self.allsips = self.db.create_table("sips", schema=_Sip, exist_ok=True)

        # TODO: put indexes

    def store_beans(self, beans: list[Bean]):
        if not beans: return 0

        to_store = [ _Bean(**bean.model_dump(exclude_none=True)) for bean in beans]        
        result = self.allbeans.merge_insert("url") \
            .when_not_matched_insert_all() \
            .execute(to_store)
        return result.num_inserted_rows

    def store_publishers(self, publishers: list[Publisher]):
        if not publishers: return 0

        to_store = [_Publisher(**publisher.model_dump(exclude_none=True)) for publisher in publishers]
        result = self.allpublishers.merge_insert("source") \
            .when_not_matched_insert_all() \
            .execute(to_store)
        return result.num_inserted_rows

    def store_chatters(self, chatters: list[Chatter]):
        if not chatters: return 0

        to_store = [_Chatter(**chatter.model_dump(exclude_none=True, exclude=["shares"])) for chatter in chatters]
        self.allchatters.add(to_store)
        return len(to_store)

    def store_mugs(self, mugs: list[Mug]):
        if not mugs: return 0

        to_store = [_Mug(**mug.model_dump(exclude_none=True)) for mug in mugs]
        result = self.allmugs.merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute(to_store)
        return result.num_inserted_rows
    
    def store_sips(self, sips: list[Sip]):
        if not sips: return 0

        to_store = [_Sip(**sip.model_dump(exclude_none=True)) for sip in sips]
        result = self.allsips.merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute(to_store)
        return result.num_inserted_rows