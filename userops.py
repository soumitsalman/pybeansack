import config
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
        return ic(_ids.insert_one(
            {
                "_id": f"{username}@{source}",
                "connected_ids": [
                    {"source": source, "userid": username}
                ]
            }
        ).inserted_id)
    
def get_preferences(source: str, username: str):
    userid = get_userid(source, username)
    if userid:
        prefs = _preferences.find_one({"_id": userid}, {"preference": 1}).get("preference")
        if prefs:
            return [item.get('text') for item in prefs if item.get('text')]


def update_userid(userid: str, source: str, username: str):
    _ids.update_one(
        {"_id": userid}, 
        { 
            "$push": {
                "connected_ids": {"source": source, "userid": username}
            }
        }
    )

def update_preferences(source: str, username: str, preference: list[str]):
    userid = get_userid(source, username, True)
    if userid:
        pref_update = {
            "preference": [{'text': item, 'direction': 'positive'} for item in preference]
        }
        ic(_preferences.update_one({"_id": userid}, {"$set": pref_update}, upsert=True).matched_count)



