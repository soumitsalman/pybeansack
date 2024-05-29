import logging
import requests
from retry import retry
from . import config
from icecream import ic
import pymongo

_DB = "users"
_IDS = "ids"
_PREFERENCES = "preferences"

def create_mongo_client(conn_str: str, db_name: str, coll_name:str):
    client = pymongo.MongoClient(conn_str)
    db = client[db_name]
    return db[coll_name]

_ids = create_mongo_client(config.get_db_connection_string(), _DB, _IDS)
_preferences = create_mongo_client(config.get_db_connection_string(), _DB, _PREFERENCES)

def get_userid(source: str, username: str, create_if_not_found: bool = False):    
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
    
def get_preference_texts(source: str, username: str):
    userid = get_userid(source, username)
    if userid:
        result = _preferences.find_one(
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

def get_preference_embeddings(source: str, username: str):
    userid = get_userid(source, username)
    if userid:
        result = _preferences.find_one(
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

def get_embeddings_for_preference(source: str, username: str, preference: str):
    userid = get_userid(source, username)
    if userid:
        result = _preferences.find_one(
            {         
                "_id": userid,
                "preference.text": preference
            },
            { "preference.$": 1 }
        )   
        if result:
            return result["preference"][0].get("embeddings")  

def update_userid(userid: str, source: str, username: str):
    _ids.update_one(
        {"_id": userid}, 
        { 
            "$push": {
                "connected_ids": {"source": source, "userid": username}
            }
        }
    )

def update_preferences(source: str, username: str, texts: list[str]):
    userid = get_userid(source, username, True)
    if userid:
        embeddings = retry_embeddings(["classification: "+t for t in texts])
        if embeddings:
            prefs = [{"text": t.title(), "embeddings": e} for t, e in zip(texts, embeddings)]           
        else:
            logging.warning("[userops] generated embeddings are NOT supposed to be none.")        
            prefs = [{'text': t.title()} for t in texts]
        _preferences.update_one({"_id": userid}, {"$set": {"preference": prefs}}, upsert=True)

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