from pymongo import MongoClient
from pymongo.collection import Collection
from .config import *
from cachetools import TTLCache, cached
from icecream import ic

DB = "espresso"
users: Collection = None
categories: Collection = None

SYSTEM = "__SYSTEM__"
SOURCE = "source"
ID = "_id"
NAME = "name"
TOPICS = "topics"
TEXT = "text"
DESCRIPTION = "description"
# EMBEDDING = "embedding"
CONNECTIONS = "connections"
PREFERENCES = "preferences"
CATEGORIES = "categories"

def initialize(conn_str: str):
    client = MongoClient(conn_str)
    global users, categories
    users = client[DB]["users"]
    categories = client[DB][CATEGORIES]

userid_filter = lambda user: {ID: user[ID]} if ID in user else {f"{CONNECTIONS}.{user[SOURCE]}": user[NAME]}

def _get_userid(user: dict) -> str:
    if ID not in user:
        user = users.find_one(userid_filter(user), projection={ID: 1})
    return user[ID] if user else None

def get_user(user: dict):
    return users.find_one(userid_filter(user))

def register_user(user, preferences) -> bool:
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
 
def get_topics(user: dict):
    items=categories.distinct(CATEGORIES, {SOURCE:_get_userid(user)})
    items.sort()
    return items

def update_topics(user: dict, topics: list[str]|dict[str, str]):
    userid = _get_userid(user)
    if userid:
        categories.delete_many({SOURCE: userid})
        if isinstance(topics, dict):
            categories.insert_many([{TEXT: topic, CATEGORIES: description.split(","), DESCRIPTION: description, SOURCE: userid } for topic, description in topics.items()], ordered=False)
        elif isinstance(topics, list) and isinstance(topics[0], str):
            categories.insert_many([{TEXT: topic, CATEGORIES: [topic], SOURCE: userid} for topic in topics], ordered=False)

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK)) 
def get_system_topics():
    return get_topics({ID: SYSTEM})


# def add_connection(userid: str|tuple[str, str], connection: tuple[str, str]):
#     _upsert_user_metadata(userid, {K_CONNECTIONS: {connection[0]: connection[1]}})

# def remove_connection(userid: str|tuple[str, str], connection):
#     users.update_one(userid_filter(userid), {"$unset": { f"{K_CONNECTIONS}.{connection}": "" }})

    


# def register_user(userid: str|tuple[str, str]):
#     userdata.insert_one(_new_userid(userid))

# def unregister_user(userid: str|tuple[str, str]):
#     users.delete_one(userid_filter(userid))

# def _new_userid(userid):
#     return {K_ID: userid} if isinstance(userid, str) else \
#         {
#             K_ID: f"{userid[1]}@{userid[0]}",
#             K_CONNECTIONS: {userid[0]: userid[1]}
#         }

# def _upsert_user_metadata(userid: str|tuple[str, str], metadata):
#     users.update_one(
#         filter = userid_filter(userid),
#         update={
#             "$setOnInsert": _new_userid(userid),
#             "$set": metadata or {}
#         },
#         upsert=True
#     )



# def get_preferences(userid):
#     return ["Cybersecurity", "Generative AI", "Robotics", "Space and Rockets", "Politics", "Yo Momma"]

# def get_default_preferences():
#     return ["Cybersecurity", "Generative AI", "Robotics", "Space and Rockets", "Politics", "Yo Momma"]

# def get_userid(username: str, source: str, create_if_not_found: bool = False):    
#     item = _ids.find_one(
#         {
#             "connected_ids": {
#                 "$elemMatch": {"source": source, "userid": username}
#             }
#         }, 
#         {"_id": 1})
#     if item:
#         return item.get("_id")
#     elif create_if_not_found:
#         return _ids.insert_one(
#             {
#                 "_id": f"{username}@{source}",
#                 "connected_ids": [
#                     {"source": source, "userid": username}
#                 ]
#             }
#         ).inserted_id
    
# def update_userid(userid: str, username: str, source: str):
#     if userid != EDITOR_USER:
#         _ids.update_one(
#             {"_id": userid}, 
#             { 
#                 "$push": {
#                     "connected_ids": {"source": source, "userid": username}
#                 }
#             }
#         )
    
# # returns the user preference text labels if something exists
# # input params are None, then it will return the global/master/editor accounts preferences
# def get_preference_texts(username: str=EDITOR_USER, source: str=None):    
#     if userid := (get_userid(source = source, username=username) if source else username):
#         result = preferences.find_one(
#             { "_id": userid },            
#             {                
#                 "texts": { 
#                     "$map": {
#                         "input": "$preference",
#                         "as": "pref",
#                         "in": "$$pref.text"
#                     }
#                 }                
#             }
#         )
#         return result["texts"] if result else None

# def get_preference_embeddings(username: str = EDITOR_USER, source: str = None):
#     if userid := (get_userid(source = source, username=username) if source else username):
#         result = preferences.find_one(
#             { "_id": userid },            
#             {                
#                 "embeddings": { 
#                     "$map": {
#                         "input": "$preference",
#                         "as": "pref",
#                         "in": "$$pref.embeddings"
#                     }
#                 }                
#             }
#         )
#         return result["embeddings"] if result else None
    
# def get_all_preferences(username: str = EDITOR_USER, source: str = None):
#     if userid := (get_userid(source = source, username=username) if source else username):
#         result = list(preferences.aggregate(
#             [
#                 {
#                     "$match": {"_id": userid}
#                 },
#                 {
#                     "$unwind": "$preference"
#                 },
#                 {
#                      "$project": {
#                         "_id": 0,
#                         "text": "$preference.text",
#                         "embeddings": "$preference.embeddings"
#                     }
#                 }
#             ]
#         ))
#         return result

# def get_selected_preferences(pref: str|list, username: str = EDITOR_USER, source: str = None):
#     if userid := (get_userid(source = source, username=username) if source else username):
#         texts = [pref] if isinstance(pref, str) else pref # make it an array
#         result = list(preferences.aggregate(
#             [
#                 {
#                     "$match": {"_id": userid}
#                 },
#                 {
#                     "$unwind": "$preference"
#                 },
#                 {
#                     "$match": {
#                         "preference.text": { "$in": texts }
#                     }
#                 },
#                 {
#                      "$project": {
#                         "_id": 0,
#                         "text": "$preference.text",
#                         "embeddings": "$preference.embeddings"
#                     }
#                 }
#             ]
#         ))
#         return result # [emb["embeddings"] for emb in result]   

# def update_topics(items: list|dict, username: str = EDITOR_USER, source: str = None):
#     if userid := (get_userid(source = source, username=username, create_if_not_found=True) if source else username):
#         if isinstance(items, list):
#             labels = items
#             embeddings = retry_embeddings([f"classification: {item}" for item in items])
#         else:    
#             labels = list(items.keys())
#             embeddings = retry_embeddings([f"classification: {item}" for item in items.values()])

#         if embeddings:
#             prefs = [{"text": t.title(), "embeddings": e} for t, e in zip(labels, embeddings)]           
#         else:
#             logging.warning("[userops] failed generating user preference embeddings.")        
#             prefs = [{'text': t.title()} for t in labels]
#         preferences.update_one({"_id": userid}, {"$set": {"preference": prefs}}, upsert=True)

# def update_topics(items: list|dict, username: str = EDITOR_USER, source: str = None):
#     if userid := (get_userid(source = source, username=username, create_if_not_found=True) if source else username):
#         if isinstance(items, list):
#             labels = items
#             embeddings = retry_embeddings([f"classification: {item}" for item in items])
#         else:    
#             labels = list(items.keys())
#             embeddings = retry_embeddings([f"classification: {item}" for item in items.values()])

#         if embeddings:
#             prefs = [{"text": t.title(), "embeddings": e} for t, e in zip(labels, embeddings)]           
#         else:
#             logging.warning("[userops] failed generating user preference embeddings.")        
#             prefs = [{'text': t.title()} for t in labels]
#         preferences.update_one({"_id": userid}, {"$set": {"preference": prefs}}, upsert=True)

# @retry(Exception, tries=5, delay=5)
# def _retry_internal(texts):
#     resp = requests.post(config.get_embedder_url(), json={"inputs": texts})
#     resp.raise_for_status()
#     result = resp.json() if (resp.status_code == requests.codes["ok"]) else None
#     if len(result) != len(texts):
#         raise Exception(f"Failed Generating Embeddings. Generated {len(result)}. Expected {len(texts)}")
#     return result

# def retry_embeddings(texts: list[str]) -> list[list[float]]:    
#     try:
#         return _retry_internal(texts)
#     except:
#         return None