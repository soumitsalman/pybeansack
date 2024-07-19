from pymongo import MongoClient
from pymongo.collection import Collection
from .espressops import get_embedding

userdata: Collection = None

EDITOR = "__EDITOR__"
K_ID = "_id"
K_TOPICS = "topics"
K_TEXT = "text"
K_DESCRIPTION = "description"
K_EMBEDDING = "embedding"
K_CONNECTIONS = "connections"

userid_filter = lambda userid: {K_ID: userid} if isinstance(userid, str) else {f"{K_CONNECTIONS}.{userid[0]}": userid[1]}

def initialize(conn_str: str):
    client = MongoClient(conn_str)
    global userdata
    userdata = client["users"]["userdata"]

def get_topics(userid: str|tuple[str, str], text_only: bool):
    if not userid:
        return None
    
    filter={
        K_TOPICS: {"$exists": True},
        "$expr": { "$gt": [{ "$size": "$topics" }, 0] }
    }
    filter.update(userid_filter(userid))
    result = userdata.find_one(
        filter=filter, 
        projection={K_TOPICS: {K_TEXT: 1} if text_only else 1}
    )
    if result:
        return [item[K_TEXT] for item in result[K_TOPICS]] if text_only else result[K_TOPICS]
    
def update_topics(userid: str|tuple[str, str], topics: list[str]|dict[str, str]):
    if isinstance(topics, dict):
        topics = {topic: {K_TEXT: topic, K_DESCRIPTION: description, K_EMBEDDING: get_embedding(topic, description) } for topic, description in topics.items()}
    elif isinstance(topics, list) and isinstance(topics[0], str):
        topics = {topic: {K_TEXT: topic, K_EMBEDDING: get_embedding(topic, None) } for topic in topics}
    else:
        return
    _upsert_user_metadata(userid, {K_TOPICS: list(topics.values())})

def add_connection(userid: str|tuple[str, str], connection: tuple[str, str]):
    _upsert_user_metadata(userid, {K_CONNECTIONS: {connection[0]: connection[1]}})

def remove_connection(userid: str|tuple[str, str], connection):
    userdata.update_one(userid_filter(userid), {"$unset": { f"{K_CONNECTIONS}.{connection}": "" }})

def register_user(userid: str|tuple[str, str]):
    userdata.insert_one(_new_userid(userid))

def unregister_user(userid: str|tuple[str, str]):
    userdata.delete_one(userid_filter(userid))

def _new_userid(userid):
    return {K_ID: userid} if isinstance(userid, str) else \
        {
            K_ID: f"{userid[1]}@{userid[0]}",
            K_CONNECTIONS: {userid[0]: userid[1]}
        }

def _upsert_user_metadata(userid: str|tuple[str, str], metadata):
    userdata.update_one(
        filter = userid_filter(userid),
        update={
            "$setOnInsert": _new_userid(userid),
            "$set": metadata or {}
        },
        upsert=True
    )



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