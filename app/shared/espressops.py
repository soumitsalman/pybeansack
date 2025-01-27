from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import Collection
from memoization import cached
from app.pybeansack.embedding import Embeddings
from app.shared.utils import *
from app.shared.datamodel import *
from icecream import ic
# from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender

SYSTEM = "__SYSTEM__"
ID = "_id"
NAME = "name"
IMAGE_URL = "image_url"
TITLE = "title"
DESCRIPTION = "description"
EMBEDDING = "embedding"
URL = "url"
CREATED = "created"
OWNER = "owner"
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
            if linked_account and linked_account not in user["linked_accounts"]:
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
            {"email": email}, 
            {
                "$addToSet": {"linked_accounts": account}
            }
        )

    def delete_user(self, email: str):
        self.users.delete_one({"_id": email})

    def follow_barista(self, email: str, barista_id: str):
        self.users.update_one(
            {"email": email}, 
            {
                "$addToSet": {"following": barista_id}
            }
        )
        return self.users.find_one({"email": email})["following"]

    def unfollow_barista(self, email: str, barista_id: str):
        self.users.update_one(
            {"email": email}, 
            {
                "$pull": {"following": barista_id}
            }
        )
        return self.users.find_one({"email": email})["following"]

    # @cached(max_size=20, ttl=ONE_HOUR) 
    def get_barista(self, id: str) -> Barista:
        barista = self.baristas.find_one({ID: id})
        if barista:
            return Barista(**barista)

    # @cached(max_size=10, ttl=ONE_HOUR) 
    def get_baristas(self, ids: list[str], projection: dict = {EMBEDDING: 0}):
        filter = {ID: {"$in": ids}} if ids else {}
        return [Barista(**barista) for barista in self.baristas.find(filter, sort={TITLE: 1}, projection=projection)]
    
    @cached(max_size=10, ttl=ONE_HOUR) 
    def sample_baristas(self, limit: int):
        pipeline = [
            { "$match": {"public": True} },
            { "$sample": {"size": limit} },
            { "$project": {ID: 1, TITLE: 1, DESCRIPTION: 1} }
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
    
    def publish(self, barista_id: str):
        return self.baristas.update_one(
            {ID: barista_id}, 
            { "$set": { "public": True } }
        ).acknowledged
        
    def unpublish(self, barista_id: str):
        return self.baristas.update_one(
            {ID: barista_id}, 
            { "$set": { "public": False } }
        ).acknowledged
        
    def is_published(self, barista_id: str):
        val = self.baristas.find_one({ID: barista_id}, {"public": 1, OWNER: 1})
        return val.get("public", val[OWNER] == SYSTEM) if val else False        

    def bookmark(self, user: User, url: str):
        return self.baristas.update_one(
            filter = {ID: user.email}, 
            update = { 
                "$addToSet": { "urls": url },
                "$setOnInsert": { 
                    OWNER: user.email,
                    TITLE: user.name,
                    DESCRIPTION: "News, blogs and posts shared by " + user.name
                }
            },
            upsert = True
        ).acknowledged
    
    def unbookmark(self, user: User, url: str):
        return self.baristas.update_one(
            filter = {ID: user.email}, 
            update = { "$pull": { "urls": url } }
        ).acknowledged
    
    def is_bookmarked(self, user: User, url: str):
        return self.baristas.find_one({ID: user.email, "urls": url})

db: EspressoDB = None
def initialize(db_connection_string: str, embedder: Embeddings):
    global db
    db = EspressoDB(db_connection_string, embedder)


# def convert_new_userid(userid):
#     return re.sub(r'[^a-zA-Z0-9]', '-', userid)
   
    
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