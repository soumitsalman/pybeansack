from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import Collection
from memoization import cached
from app.pybeansack.embedding import Embeddings
from app.shared.utils import *
from app.shared.datamodel import *
# from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender

SYSTEM = "__SYSTEM__"
ID = "_id"
NAME = "name"
IMAGE_URL = "image_url"
TITLE = "title"
DESCRIPTION = "description"
EMBEDDING = "embedding"

DEFAULT_BARISTAS = [
    "artificial-intelligence",
    "automotive",
    "aviation---aerospace",
    "business---finance",
    "career---professional-skills",
    "cryptocurrency---blockchain",
    "cybersecurity",    
    # "environment---clean-energy",
    # "food---health",
    "gadgets---iot",
    "government---politics",
    "hackernews",
    "hpc---datacenters",
    # "leadership---people-management",
    # "logistics---transportation",
    "reddit",
    "robotics---manufacturing",
    "science---mathematics",
    "software-engineering",
    "solar-energy",
    "startups---vcs",
    # "video-games---virtual-reality"
]

class EspressoDB: 
    users: Collection
    baristas: Collection
    embedder: Embeddings = None

    def __init__(self, db_conn_str: str, embedder: Embeddings):
        client = MongoClient(db_conn_str)
        self.users = client["espresso"]["users"]
        self.baristas = client["espresso"]["baristas"]
        self.embedder = embedder

    # @cached(max_size=10, ttl=ONE_DAY) 
    def get_user(self, email: str, linked_account: str = None) -> User|None:
        user = self.users.find_one({"email": email})
        if user:
            if linked_account:
                self.link_account(email, linked_account)
            return User(**user)
        
    def create_user(self, userinfo: dict):
        user = User(
            id=userinfo["email"], 
            email=userinfo["email"], 
            name=userinfo["name"], 
            image_url=userinfo.get("picture"), 
            created=datetime.now(),
            updated=datetime.now(),
            linked_accounts=[userinfo["iss"]],
            following=DEFAULT_BARISTAS
        )
        self.users.insert_one(user.model_dump(exclude_none=True, by_alias=True))
        return user

    def link_account(self, email: str, account: str):
        self.users.update_one(
            {"_id": email}, 
            {
                "$addToSet": {"linked_accounts": account},
                "$set": {"updated": datetime.now()}
            }
        )

    def delete_user(self, email: str):
        self.users.delete_one({"_id": email})

    # @cached(max_size=1000, ttl=ONE_HOUR) 
    def get_barista(self, id: str) -> Barista:
        return Barista(**self.baristas.find_one({ID: id}))

    # @cached(max_size=10, ttl=ONE_HOUR) 
    def get_baristas(self, ids: list[str], projection: dict = {EMBEDDING: 0}):
        filter = {ID: {"$in": ids}} if ids else {}
        return [Barista(**barista) for barista in self.baristas.find(filter, sort={TITLE: 1}, projection=projection)]
    
    @cached(max_size=10, ttl=ONE_HOUR) 
    def sample_baristas(self, limit: int):
        pipeline = [
            { "$sample": {"size": limit} },
            { "$project": {ID: 1, TITLE: 1, DESCRIPTION: 1, IMAGE_URL: 1} }
        ]
        return [Barista(**barista) for barista in self.baristas.aggregate(pipeline)]
    
    @cached(max_size=10, ttl=ONE_HOUR) 
    def get_following_baristas(self, user: User):
        following = self.users.find_one({ID: user.email}, {"following": 1})
        if following:
            return self.get_baristas(following["following"])

    @cached(max_size=10, ttl=ONE_HOUR) 
    def search_baristas(self, query: str|list[str]):
        pipeline = [
            {   "$match": {"$text": {"$search": query if isinstance(query, str) else " ".join(query)}} },            
            {   "$addFields":  { "search_score": {"$meta": "textScore"}} },
            {   "$project": {"embedding": 0} },
            {   "$sort": {"search_score": -1} },
            {   "$limit": 10 }     
        ]        
        return [Barista(**barista) for barista in self.baristas.aggregate(pipeline)]

db: EspressoDB = None
def initialize(db_connection_string: str, sb_connection_string: str, embedder: Embeddings):
    global db
    db = EspressoDB(db_connection_string, embedder)

# def initialize(conn_str: str, sb_conn_str: str, emb: Embeddings):    
#     global users, categories, baristas  
#     client = MongoClient(conn_str)
#     users = client[DB]["users"]
#     categories = client[DB][CATEGORIES]
#     baristas = client[DB][BARISTAS]

#     global index_queue
#     index_queue = ServiceBusClient.from_connection_string(sb_conn_str).get_queue_sender(COLLECT_QUEUE)

#     global embedder
#     embedder = emb

# userid_filter = lambda user: {ID: user[ID]} if ID in user else {f"{CONNECTIONS}.{user[SOURCE]}": user.get(SOURCE_ID)}

# def convert_new_userid(userid):
#     return re.sub(r'[^a-zA-Z0-9]', '-', userid)

# def _get_userid(user: dict) -> str:
#     if ID not in user:
#         user = users.find_one(userid_filter(user), projection={ID: 1})
#     return user[ID] if user else None

# def get_user(user: dict):
#     return users.find_one(userid_filter(user))

# def register_user(user, preferences) -> dict:
#     if user:
#         new_user = {
#             ID: convert_new_userid(user[ID] if ID in user else f"{user[NAME]}@{user[SOURCE]}"),
#             IMAGE_URL: user.get(IMAGE_URL),
#             CONNECTIONS: {
#                 user[SOURCE]: user[SOURCE_ID]
#             },
#             PREFERENCES: {
#                 'last_ndays': preferences['last_ndays']
#             }            
#         }
#         users.update_one(filter={ID: new_user[ID]}, update={"$setOnInsert": new_user}, upsert=True)
#         update_categories(new_user, preferences['topics'])
#         return new_user

# def unregister_user(user) -> bool:
#     userid = _get_userid(user)
#     update_categories(user, [])
#     baristas.delete_many({SOURCE: userid})
#     users.delete_one({ID: userid})

# # TODO: implement following pages
# def get_following_baristas(user: dict):
#     return None

# # TODO: implement trending pages
# def get_trending_baristas():
#     return None

# @cached(max_size=1000, ttl=ONE_HOUR) 
# def get_barista(id: str):
#     return Barista(**baristas.find_one({ID: id})) if id else None

# @cached(max_size=10, ttl=ONE_DAY) 
# def get_baristas(ids: list[str]):
#     filter = {ID: {"$in": ids}} if ids else {}
#     return [Barista(**barista) for barista in baristas.find(filter, sort={TITLE: 1})]

# def search_baristas(query: str|list[str]):
#     pipeline = [
#         {   "$match": {"$text": {"$search": query if isinstance(query, str) else " ".join(query)}} },            
#         {   "$addFields":  { K_SEARCH_SCORE: {"$meta": "textScore"}} },
#         {   "$sort": {K_SEARCH_SCORE: -1} },
#         {   "$limit": 10 }     
#     ]        
#     return [Barista(**barista) for barista in baristas.aggregate(pipeline)]

# def get_categories(user: dict):
#     return list(categories.find(filter={SOURCE:_get_userid(user)}, sort={TEXT: 1}, projection={ID: 1, TEXT: 1}))

# def get_user_category_ids(user: dict):
#     return [topic[K_ID] for topic in get_categories(user)]

# @cached(max_size=1, ttl=ONE_WEEK) 
# def get_system_categories():
#     return get_categories({ID: SYSTEM})

# def get_system_topic_id_label():    
#     return {topic[K_ID]: topic[K_TEXT] for topic in get_system_categories()}

# @cached(max_size=100, ttl=ONE_HOUR) 
# def get_preferences(user: dict):
#     userdata = get_user(user)
#     return {
#         "topics": get_categories(user),
#         "last_ndays": userdata[PREFERENCES]['last_ndays']        
#     } if userdata else None

# def update_preferences(user: dict, preferences: dict):
#     if user:
#         users.update_one(
#             filter=userid_filter(user), 
#             update={
#                 "$set": {
#                     PREFERENCES: {'last_ndays': preferences['last_ndays']} 
#                 }
#             })
#         update_categories(user, preferences['topics'])

# def update_categories(user: dict, new_categories: list[str]):
#     userid = _get_userid(user)
#     if userid:
#         updates = []   
#         query = {
#             "$or": [
#                 {ID: {"$in": new_categories}},
#                 {SOURCE: userid}
#             ]
#         }     
#         for existing in categories.find(query, {ID: 1, SOURCE: 1}):
#             sources = existing[SOURCE] if isinstance(existing[SOURCE], list) else [existing[SOURCE]]
#             sources.append(userid) if existing[ID] in new_categories else sources.remove(userid)                
#             updates.append(UpdateOne(
#                 filter = {ID: existing[ID]}, 
#                 update = {"$set": { SOURCE: list(set(sources))}}
#             ))            
#         categories.bulk_write(updates, ordered=False)

# def add_connection(user: dict, connection: dict):
#     users.update_one(
#         filter = userid_filter(user), 
#         update = {
#             "$set": { f"{CONNECTIONS}.{connection[SOURCE]}": connection[SOURCE_ID] }
#         })

# def remove_connection(user: dict, source):
#     users.update_one(
#         filter = userid_filter(user), 
#         update = {
#             "$unset": { f"{CONNECTIONS}.{source}": "" } 
#         })
    


# def match_categories(content):
#     cats = []
#     for chunk in utils.chunk(content, embedder.context_len):   
#         pipeline = [
#             {
#                 "$search": {
#                     "cosmosSearch": {
#                         "vector": embedder(chunk),
#                         "path":   EMBEDDING,
#                         "k":      20,
#                         "filter": {SOURCE: SYSTEM}
#                     },
#                     "returnStoredSource": True
#                 }
#             },
#             {"$addFields": { "search_score": {"$meta": "searchScore"} }},
#             {"$match": { "search_score": {"$gte": 0.79} }}, # TODO: play with this number later
#             {"$project": {EMBEDDING: 0}}
#         ] 
#         [cats.extend(cat["related"]+[cat[ID]]) for cat in categories.aggregate(pipeline)]
#     return list(set(cats))    

# @cached(max_size=1000, ttl=ONE_WEEK) 
# def channel_content(channel_id: dict):
#     return baristas.distinct(K_URL, filter = {K_SOURCE: channel_id})  

# def publish(user: dict, urls: str|list[str]):
#     userid = _get_userid(user)
#     if userid and urls:
#         urls = [urls] if isinstance(urls, str) else urls
#         entries = [{K_ID: f"{userid}:{url}", K_SOURCE: userid, K_URL: url, K_CREATED: int(time.time())} for url in urls]
#         # save to the user's channged
#         res = baristas.bulk_write([UpdateOne(filter={K_ID: entry[K_ID]}, update={"$setOnInsert": entry}, upsert=True) for entry in entries], ordered=False).acknowledged
#         index_queue.send_messages([ServiceBusMessage(json.dumps(entry)) for entry in entries])
#         return res
    
    
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