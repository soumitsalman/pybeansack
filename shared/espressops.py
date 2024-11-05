from itertools import chain
import json
import re
import time
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pybeansack import utils
from pybeansack.embedding import Embeddings
from shared.utils import *
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender
from icecream import ic
from memoization import cached

DB = "espresso"
users: Collection = None
categories: Collection = None
baristas: Collection = None
index_queue: ServiceBusSender = None
embedder: Embeddings = None

SYSTEM = "__SYSTEM__"
SOURCE = "source"
ID = "_id"
NAME = "name"
IMAGE_URL = "image_url"
SOURCE_ID = "id_in_source"
TOPICS = "topics"
TEXT = "text"
TITLE = "title"
DESCRIPTION = "description"
EMBEDDING = "embedding"
CONNECTIONS = "connections"
PREFERENCES = "preferences"
CATEGORIES = "categories"
BARISTAS = "baristas"
COLLECT_QUEUE = "collect-queue"

# INDEX DEFINITION
# db.baristas.createIndex(
#     {
#         title: "text",
#         description: "text",
#         query: "text",
#         tags: "text",
#         kinds: "text",
#         sources: "text"
#     },
#     {
#         name: "baristas_text_search"
#     }
# )
class Barista(BaseModel):
    id: str = Field(default=None, alias=ID)
    owner: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    query: Optional[str] = None
    urls: Optional[list[str]] = None
    tags: Optional[list[str]] = None
    kinds: Optional[list[str]] = None
    sources: Optional[list[str]] = None
    last_ndays: Optional[int] = None

def initialize(conn_str: str, sb_conn_str: str, emb: Embeddings):    
    global users, categories, baristas  
    client = MongoClient(conn_str)
    users = client[DB]["users"]
    categories = client[DB][CATEGORIES]
    baristas = client[DB][BARISTAS]

    global index_queue
    index_queue = ServiceBusClient.from_connection_string(sb_conn_str).get_queue_sender(COLLECT_QUEUE)

    global embedder
    embedder = emb

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
        users.update_one(filter={ID: new_user[ID]}, update={"$setOnInsert": new_user}, upsert=True)
        update_categories(new_user, preferences['topics'])
        return new_user

def unregister_user(user) -> bool:
    userid = _get_userid(user)
    update_categories(user, [])
    baristas.delete_many({SOURCE: userid})
    users.delete_one({ID: userid})

# TODO: implement following pages
def get_following_baristas(user: dict):
    return None

# TODO: implement trending pages
def get_trending_baristas():
    return None

@cached(max_size=1000, ttl=ONE_HOUR) 
def get_barista(id: str):
    return Barista(**baristas.find_one({ID: id})) if id else None

@cached(max_size=10, ttl=ONE_DAY) 
def get_baristas(ids: list[str]):
    filter = {ID: {"$in": ids}} if ids else {}
    return [Barista(**barista) for barista in baristas.find(filter, sort={TITLE: 1})]

def search_baristas(query: str|list[str]):
    pipeline = [
        {   "$match": {"$text": {"$search": query if isinstance(query, str) else " ".join(query)}} },            
        {   "$addFields":  { K_SEARCH_SCORE: {"$meta": "textScore"}} },
        {   "$sort": {K_SEARCH_SCORE: -1} },
        {   "$limit": 10 }     
    ]        
    return [Barista(**barista) for barista in baristas.aggregate(pipeline)]

def get_categories(user: dict):
    return list(categories.find(filter={SOURCE:_get_userid(user)}, sort={TEXT: 1}, projection={ID: 1, TEXT: 1}))

def get_user_category_ids(user: dict):
    return [topic[K_ID] for topic in get_categories(user)]

@cached(max_size=1, ttl=ONE_WEEK) 
def get_system_categories():
    return get_categories({ID: SYSTEM})

def get_system_topic_id_label():    
    return {topic[K_ID]: topic[K_TEXT] for topic in get_system_categories()}

@cached(max_size=100, ttl=ONE_HOUR) 
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
    


def match_categories(content):
    cats = []
    for chunk in utils.chunk(content, embedder.context_len):   
        pipeline = [
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": embedder(chunk),
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

@cached(max_size=1000, ttl=ONE_WEEK) 
def channel_content(channel_id: dict):
    return baristas.distinct(K_URL, filter = {K_SOURCE: channel_id})  

def publish(user: dict, urls: str|list[str]):
    userid = _get_userid(user)
    if userid and urls:
        urls = [urls] if isinstance(urls, str) else urls
        entries = [{K_ID: f"{userid}:{url}", K_SOURCE: userid, K_URL: url, K_CREATED: int(time.time())} for url in urls]
        # save to the user's channged
        res = baristas.bulk_write([UpdateOne(filter={K_ID: entry[K_ID]}, update={"$setOnInsert": entry}, upsert=True) for entry in entries], ordered=False).acknowledged
        index_queue.send_messages([ServiceBusMessage(json.dumps(entry)) for entry in entries])
        return res
    
    
