from itertools import chain
import time
from pymongo import MongoClient
from pymongo.collection import Collection
from shared import llmops
from shared.config import *
from cachetools import TTLCache, cached
from icecream import ic

DB = "espresso"
users: Collection = None
categories: Collection = None
channels: Collection = None

SYSTEM = "__SYSTEM__"
SOURCE = "source"
ID = "_id"
NAME = "name"
SOURCE_ID = "id_in_source"
TOPICS = "topics"
TEXT = "text"
DESCRIPTION = "description"
EMBEDDING = "embedding"
CONNECTIONS = "connections"
PREFERENCES = "preferences"
CATEGORIES = "categories"

def initialize(conn_str: str):
    client = MongoClient(conn_str)
    global users, categories, channels
    users = client[DB]["users"]
    categories = client[DB][CATEGORIES]
    channels = client[DB]["channels"]

userid_filter = lambda user: {ID: user[ID]} if ID in user else {f"{CONNECTIONS}.{user[SOURCE]}": user.get(SOURCE_ID)}

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
                user[SOURCE]: user[SOURCE_ID]
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
    return get_topics({ID: SYSTEM})+[UNCATEGORIZED]

@cached(TTLCache(maxsize=100, ttl=ONE_HOUR)) 
def get_preferences(user: dict):
    userdata = get_user(user)
    return {
        "topics": get_topics(user),
        "last_ndays": userdata[PREFERENCES]['last_ndays']        
    } if userdata else None

def update_preferences(user: dict, preferences: dict):
    if user:
        users.update_one(
            filter=userid_filter(user), 
            update={
                "$set": {
                    PREFERENCES: {'last_ndays': preferences['last_ndays']} 
                }
            })
        update_topics(user, preferences['topics'])

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
            "$set": { f"{CONNECTIONS}.{connection[SOURCE]}": connection[SOURCE_ID] }
        })

def remove_connection(user: dict, source):
    users.update_one(
        filter = userid_filter(user), 
        update = {
            "$unset": { f"{CONNECTIONS}.{source}": "" } 
        })

def search_categories(content):    
    pipeline = [
        {
            "$search": {
                "cosmosSearch": {
                    "vector": llmops.embed(content),
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

def publish(user: dict, url: str):
    userid = _get_userid(user)
    if userid and url:
        id = f"{userid}:{url}"
        entry = {K_ID: id, K_SOURCE: userid, K_URL: url, K_UPDATED: int(time.time())}
        res = channels.update_one(filter = {K_ID: id}, update = {"$setOnInsert": entry}, upsert=True).acknowledged
        # TODO: push url to index-queue
        return res
    