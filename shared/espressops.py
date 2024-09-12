from itertools import chain
import re
import time
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pybeansack import utils
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
IMAGE_URL = "image_url"
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

def convert_new_userid(userid):
    return re.sub(r'[^a-zA-Z0-9]', '-', userid)

def _get_userid(user: dict) -> str:
    if ID not in user:
        user = users.find_one(userid_filter(user), projection={ID: 1})
    return user[ID] if user else None

def get_user(user: dict):
    return users.find_one(userid_filter(user))

def register_user(user, preferences) -> dict:
    if user:
        new_user = {
            ID: convert_new_userid(user[ID] if ID in user else f"{user[NAME]}@{user[SOURCE]}"),
            IMAGE_URL: user.get(IMAGE_URL),
            CONNECTIONS: {
                user[SOURCE]: user[SOURCE_ID]
            },
            PREFERENCES: {
                'last_ndays': preferences['last_ndays']
            }            
        }
        users.insert_one(new_user)
        update_categories(new_user, preferences['topics'])
        return new_user

def unregister_user(user) -> bool:
    userid = _get_userid(user)
    update_categories(user, [])
    channels.delete_many({SOURCE: userid})
    users.delete_one({ID: userid})
    
def get_categories(user: dict):
    return list(categories.find(filter={SOURCE:_get_userid(user)}, sort={TEXT: 1}, projection={ID: 1, TEXT: 1}))

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK)) 
def get_system_categories():
    return get_categories({ID: SYSTEM})

@cached(TTLCache(maxsize=100, ttl=ONE_HOUR)) 
def get_preferences(user: dict):
    userdata = get_user(user)
    return {
        "topics": get_categories(user),
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
        update_categories(user, preferences['topics'])

def update_categories(user: dict, new_categories: list[str]):
    userid = _get_userid(user)
    if userid:
        updates = []   
        query = {
            "$or": [
                {ID: {"$in": new_categories}},
                {SOURCE: userid}
            ]
        }     
        for existing in categories.find(query, {ID: 1, SOURCE: 1}):
            sources = existing[SOURCE] if isinstance(existing[SOURCE], list) else [existing[SOURCE]]
            sources.append(userid) if existing[ID] in new_categories else sources.remove(userid)                
            updates.append(UpdateOne(
                filter = {ID: existing[ID]}, 
                update = {"$set": { SOURCE: list(set(sources))}}
            ))            
        categories.bulk_write(updates, ordered=False)

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
    
@cached(TTLCache(maxsize=1000, ttl=ONE_WEEK)) 
def category_label(id: str):
    if id:
        res = categories.find_one(filter = {ID: id}, projection={TEXT: 1})
        return res[TEXT] if res else None

def match_categories(content):
    cats = []
    for chunk in utils.chunk(content, 496):   
        pipeline = [
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": llmops.embed(chunk),
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
        [cats.extend(cat["related"]+[cat[ID]]) for cat in categories.aggregate(pipeline)]
    return list(set(cats))    
    

def publish(user: dict, url: str):
    userid = _get_userid(user)
    if userid and url:
        id = f"{userid}:{url}"
        entry = {K_ID: id, K_SOURCE: userid, K_URL: url, K_UPDATED: int(time.time())}
        res = channels.update_one(filter = {K_ID: id}, update = {"$setOnInsert": entry}, upsert=True).acknowledged
        # TODO: push url to index-queue
        return res

def channel_content(channel_id: dict):
    return channels.distinct(K_URL, filter = {K_SOURCE: channel_id})
    
