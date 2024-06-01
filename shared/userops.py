import logging
import requests
from retry import retry
from . import config
from icecream import ic
import pymongo

_DB = "users"
_IDS = "ids"
_PREFERENCES = "preferences"
EDITOR_USER = "__EDITOR__"

def create_mongo_client(conn_str: str, db_name: str, coll_name:str):
    client = pymongo.MongoClient(conn_str)
    db = client[db_name]
    return db[coll_name]

_ids = create_mongo_client(config.get_db_connection_string(), _DB, _IDS)
preferences = create_mongo_client(config.get_db_connection_string(), _DB, _PREFERENCES)

def get_userid(username: str, source: str, create_if_not_found: bool = False):    
    item = _ids.find_one(
        {
            "connected_ids": {
                "$elemMatch": {"source": source, "userid": username}
            }
        }, 
        {"_id": 1})
    if item:
        return item.get("_id")
    elif create_if_not_found:
        return _ids.insert_one(
            {
                "_id": f"{username}@{source}",
                "connected_ids": [
                    {"source": source, "userid": username}
                ]
            }
        ).inserted_id
    
def update_userid(userid: str, username: str, source: str):
    if userid != EDITOR_USER:
        _ids.update_one(
            {"_id": userid}, 
            { 
                "$push": {
                    "connected_ids": {"source": source, "userid": username}
                }
            }
        )
    
# returns the user preference text labels if something exists
# input params are None, then it will return the global/master/editor accounts preferences
def get_preference_texts(username: str=EDITOR_USER, source: str=None):    
    if userid := (get_userid(source = source, username=username) if source else username):
        result = preferences.find_one(
            { "_id": userid },            
            {                
                "texts": { 
                    "$map": {
                        "input": "$preference",
                        "as": "pref",
                        "in": "$$pref.text"
                    }
                }                
            }
        )
        return result["texts"] if result else None

def get_preference_embeddings(username: str = EDITOR_USER, source: str = None):
    if userid := (get_userid(source = source, username=username) if source else username):
        result = preferences.find_one(
            { "_id": userid },            
            {                
                "embeddings": { 
                    "$map": {
                        "input": "$preference",
                        "as": "pref",
                        "in": "$$pref.embeddings"
                    }
                }                
            }
        )
        return result["embeddings"] if result else None
    
def get_all_preferences(username: str = EDITOR_USER, source: str = None):
    if userid := (get_userid(source = source, username=username) if source else username):
        result = list(preferences.aggregate(
            [
                {
                    "$match": {"_id": userid}
                },
                {
                    "$unwind": "$preference"
                },
                {
                     "$project": {
                        "_id": 0,
                        "text": "$preference.text",
                        "embeddings": "$preference.embeddings"
                    }
                }
            ]
        ))
        return result

def get_selected_preferences(pref: str|list, username: str = EDITOR_USER, source: str = None):
    if userid := (get_userid(source = source, username=username) if source else username):
        texts = [pref] if isinstance(pref, str) else pref # make it an array
        result = list(preferences.aggregate(
            [
                {
                    "$match": {"_id": userid}
                },
                {
                    "$unwind": "$preference"
                },
                {
                    "$match": {
                        "preference.text": { "$in": texts }
                    }
                },
                {
                     "$project": {
                        "_id": 0,
                        "text": "$preference.text",
                        "embeddings": "$preference.embeddings"
                    }
                }
            ]
        ))
        return result # [emb["embeddings"] for emb in result]   

def update_preferences(items: list|dict, username: str = EDITOR_USER, source: str = None):
    if userid := (get_userid(source = source, username=username, create_if_not_found=True) if source else username):
        if isinstance(items, list):
            labels = items
            embeddings = retry_embeddings([f"classification: {item}" for item in items])
        else:    
            labels = list(items.keys())
            embeddings = retry_embeddings([f"classification: {item}" for item in items.values()])

        if embeddings:
            prefs = [{"text": t.title(), "embeddings": e} for t, e in zip(labels, embeddings)]           
        else:
            logging.warning("[userops] failed generating user preference embeddings.")        
            prefs = [{'text': t.title()} for t in labels]
        preferences.update_one({"_id": userid}, {"$set": {"preference": prefs}}, upsert=True)

@retry(Exception, tries=5, delay=5)
def _retry_internal(texts):
    resp = requests.post(config.get_embedder_url(), json={"inputs": texts})
    resp.raise_for_status()
    result = resp.json() if (resp.status_code == requests.codes["ok"]) else None
    if len(result) != len(texts):
        raise Exception(f"Failed Generating Embeddings. Generated {len(result)}. Expected {len(texts)}")
    return result

def retry_embeddings(texts: list[str]) -> list[list[float]]:    
    try:
        return _retry_internal(texts)
    except:
        return None