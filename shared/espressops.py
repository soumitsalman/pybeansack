from pymongo import MongoClient
from pymongo.collection import Collection
from langchain_core.embeddings import Embeddings
from cachetools import TTLCache, cached

# cached for 8 hours
EIGHT_HOUR = 28000
ONE_DAY = 86400
ONE_WEEK = 604800
CACHE_SIZE = 1000

K_ID = "_id"
K_EMBEDDING = "embedding"

cache: Collection = None
embedder: Embeddings = None

def initialize(conn_str: str, emb):
    client = MongoClient(conn_str)
    global cache, embedder
    cache = client["espresso"]["nlpcache"]
    embedder = emb

@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_WEEK))
def get_embedding(topic, description):
    result = cache.find_one(
        filter = {
            K_ID: K_EMBEDDING,
            topic: {"$exists": True}
        }, 
        projection={topic:1})
    if not result:
        emb = embedder.embed_query(description or topic)
        cache.update_one(
            filter={K_ID: K_EMBEDDING},
            update={
                "$setOnInsert": {K_ID: K_EMBEDDING},
                "$set": {topic:emb}
            },
            upsert=True)
        return emb
    else:
        return result[topic]