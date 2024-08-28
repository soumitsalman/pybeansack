from itertools import chain
from pymongo import MongoClient
from pymongo.collection import Collection
from pybeansack import utils
from pybeansack.embedding import BeansackEmbeddings
from .config import *
from cachetools import TTLCache, cached
from icecream import ic

DB = "espresso"
users: Collection = None
categories: Collection = None
embedder = None

SYSTEM = "__SYSTEM__"
SOURCE = "source"
ID = "_id"
NAME = "name"
TOPICS = "topics"
TEXT = "text"
DESCRIPTION = "description"
EMBEDDING = "embedding"
CONNECTIONS = "connections"
PREFERENCES = "preferences"
CATEGORIES = "categories"

def initialize(conn_str: str, emb: BeansackEmbeddings):
    client = MongoClient(conn_str)
    global users, categories, embedder
    users = client[DB]["users"]
    categories = client[DB][CATEGORIES]
    embedder = emb

userid_filter = lambda user: {ID: user[ID]} if ID in user else {f"{CONNECTIONS}.{user[SOURCE]}": user[NAME]}

def _get_userid(user: dict) -> str:
    if ID not in user:
        user = users.find_one(userid_filter(user), projection={ID: 1})
    return user[ID] if user else None

def get_user(user: dict):
    return users.find_one(userid_filter(user))

def register_user(user, preferences) -> dict:
    if user:
        new_user = {
            ID: f"{user[NAME]}@{user[SOURCE]}",
            CONNECTIONS: {
                user[SOURCE]: user[NAME]
            },
            PREFERENCES: {
                'last_ndays': preferences['last_ndays']
            }            
        }
        users.insert_one(new_user)
        update_topics(new_user, preferences['topics'])
        return new_user

def unregister_user(user) -> bool:
    userid = _get_userid(user)
    categories.delete_many({SOURCE: userid})
    users.delete_one({ID: userid})
 
def get_topics(user: dict):
    items=categories.distinct(CATEGORIES, {SOURCE:_get_userid(user)})
    items.sort()
    return items

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK)) 
def get_system_topics():
    return get_topics({ID: SYSTEM})

def update_topics(user: dict, topics: list[str]|dict[str, str]):
    userid = _get_userid(user)
    if userid:
        categories.delete_many({SOURCE: userid})
        if isinstance(topics, dict):
            categories.insert_many([{TEXT: topic, CATEGORIES: description.split(","), DESCRIPTION: description, SOURCE: userid } for topic, description in topics.items()], ordered=False)
        elif isinstance(topics, list) and isinstance(topics[0], str):
            categories.insert_many([{TEXT: topic, CATEGORIES: [topic], SOURCE: userid} for topic in topics], ordered=False)

def add_connection(user: dict, connection: dict):
    users.update_one(
        filter = userid_filter(user), 
        update = {
            "$set": { f"{CONNECTIONS}.{connection[SOURCE]}": connection[NAME] }
        })

def remove_connection(user: dict, source):
    users.update_one(
        filter = userid_filter(user), 
        update = {
            "$unset": { f"{CONNECTIONS}.{source}": "" } 
        })

def search_categories(query):    
    pipeline = [
        {
            "$search": {
                "cosmosSearch": {
                    "vector": embedder.embed_query(utils.truncate(query, EMBEDDER_CTX)),
                    "path":   EMBEDDING,
                    "k":      20,
                    "filter": {SOURCE: SYSTEM}
                },
                "returnStoredSource": True
            }
        },
        {"$addFields": { "search_score": {"$meta": "searchScore"} }},
        {"$match": { "search_score": {"$gte": 0.79} }}, # TODO: play with this number later
        {"$project": {EMBEDDING: 0}}
    ] 
    matches = categories.aggregate(pipeline)
    return list(set(chain(*(cat[CATEGORIES] for cat in matches))))