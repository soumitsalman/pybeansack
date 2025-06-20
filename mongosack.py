############################
## BEANSACK DB OPERATIONS ##
############################
import os
import logging
import re
from icecream import ic
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.database import Database
from pymongo.collection import Collection
from bson import SON
from .models import *

log = logging.getLogger(__name__)

TIMEOUT = 300000 # 3 mins
DEFAULT_VECTOR_SEARCH_SCORE = 0.75
DEFAULT_VECTOR_SEARCH_LIMIT = 1000

# names of db and collections
BEANS = "beans"
CHATTERS = "chatters"
SOURCES = "sources"
PAGES = "pages"
USERS = "users"

# LAST_UPDATED = {K_UPDATED: -1}
NEWEST = {K_CREATED: -1}
# LATEST = SON([(K_CREATED, -1), (K_TRENDSCORE, -1)])
# TRENDING = SON([(K_UPDATED, -1), (K_TRENDSCORE, -1)])
LATEST = {K_CREATED: -1, K_TRENDSCORE: -1}
TRENDING = {K_UPDATED: -1, K_TRENDSCORE: -1}
_BY_TRENDSCORE = {K_TRENDSCORE: -1}
_BY_SEARCH_SCORE = {K_SEARCH_SCORE: -1}

VALUE_EXISTS = { "$exists": True, "$ne": None}

now = lambda: datetime.now(timezone.utc)
ndays_ago = lambda ndays: now() - timedelta(days=ndays)

field_value = lambda items: {"$in": items} if isinstance(items, list) else items
lower_case = lambda items: {"$in": [item.lower() for item in items]} if isinstance(items, list) else items.lower()
case_insensitive = lambda items: {"$in": [re.compile(item, re.IGNORECASE) for item in items]} if isinstance(items, list) else re.compile(items, re.IGNORECASE)

_create_group_by = lambda field, sort_by: [
    {
        "$group": {
            "_id": { field: f"${field}" },
            "doc": { "$first": "$$ROOT" }
        }
    },
    {
        "$replaceRoot": { "newRoot": "$doc" }
    }
] + ([{"$sort": sort_by}] if sort_by else [])

def _beans_query_pipeline(filter: dict, group_by: str|list[str], sort_by, skip: int, limit: int, project: dict, count: bool):
    pipeline = []
    if filter: pipeline.append({"$match": filter})
    if sort_by: pipeline.append({"$sort": sort_by})        
    if group_by: 
        group_by = [group_by] if isinstance(group_by, str) else group_by
        [pipeline.extend(_create_group_by(gr, sort_by)) for gr in group_by]
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})
    return pipeline

def _beans_vector_search_pipeline(embedding: list[float], similarity_score: float, filter: dict, group_by: str|list[str], sort_by, skip: int, limit: int, project: dict, count: bool):    
    pipeline = [            
        {
            "$search": {
                "cosmosSearch": {
                    "vector": embedding,
                    "path":   K_EMBEDDING,
                    "filter": filter or {},
                    # if there is no group_by then limit the search set else cast a wider net
                    "k":      skip+limit if limit and not group_by else DEFAULT_VECTOR_SEARCH_LIMIT,
                }
            }
        },
        {
            "$addFields": { "search_score": {"$meta": "searchScore"} }
        }
    ]  
    if similarity_score: pipeline.append(
        {
            "$match": { "search_score": {"$gte": similarity_score} }
        }
    )
    if sort_by: pipeline.append({"$sort": sort_by})        
    if group_by: 
        group_by = [group_by] if isinstance(group_by, str) else group_by
        [pipeline.extend(_create_group_by(gr, sort_by)) for gr in group_by]
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})
    return pipeline

def _related_beans_pipeline(url, filter, sort_by, skip, limit, project, count):
    related_filter = {K_ID: {"$ne": url}}
    if filter: related_filter.update(filter)

    pipeline = [
        {
            "$match": {
                K_ID: url,
                K_CLUSTER_ID: VALUE_EXISTS
            }
        },
        {
            "$lookup": {
                "from": BEANS,
                "localField": K_CLUSTER_ID,
                "foreignField": K_CLUSTER_ID,
                "as": 'cluster',
                "pipeline": _beans_query_pipeline(related_filter, group_by=None, sort_by=sort_by, skip=skip, limit=limit, project=project, count=count)
            }
        },    
        {   
            "$unwind": "$cluster"
        },
        {
            "$replaceRoot": { "newRoot": "$cluster" }
        }
    ]
    return pipeline

def _beans_text_search_pipeline(text: str, filter: dict, group_by: str|list[str], sort_by, skip: int, limit: int, project: dict, count: bool):
    match = {"$text": {"$search": text}}
    if filter: match.update(filter)
    pipeline = [
        { "$match": match },            
        { "$addFields":  { K_SEARCH_SCORE: {"$meta": "textScore"}} },
        { "$sort": sort_by or _BY_SEARCH_SCORE }
    ]   
    if group_by: 
        group_by = [group_by] if isinstance(group_by, str) else group_by
        [pipeline.extend(_create_group_by(gr, sort_by)) for gr in group_by] 
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})    
    return pipeline

class Beansack:
    db: Database
    beanstore: Collection
    chatterstore: Collection
    sourcestore: Collection
    userstore: Collection
    pagestore: Collection

    def __init__(self, 
        conn_str: str = os.getenv("REMOTE_DB_CONNECTION_STRING", "mongodb://localhost:27017"), 
        db_name: str = os.getenv("DB_NAME", "beansack")
    ):  
        self.db = MongoClient(
            conn_str, 
            timeoutMS=TIMEOUT,
            serverSelectionTimeoutMS=TIMEOUT,
            socketTimeoutMS=TIMEOUT,
            connectTimeoutMS=TIMEOUT,
            retryWrites=True,
            minPoolSize=10,
            maxPoolSize=100)[db_name]        
        self.beanstore: Collection = self.db[BEANS]
        self.chatterstore: Collection = self.db[CHATTERS]        
        self.sourcestore: Collection = self.db[SOURCES]  
        self.userstore = self.db[USERS]
        self.pagestore = self.db[PAGES]

    ###################
    ## BEANS STORING ##
    ###################
    def store_beans(self, beans: list[Bean]) -> int:   
        # beans = self.not_exists(beans)
        if not beans: return 0
        res = self.beanstore.insert_many([bean.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for bean in beans], ordered=False)            
        return len(res.inserted_ids)

    def exists(self, beans: list[Bean]):
        if not beans: return beans
        return [item[K_ID] for item in self.beanstore.find({K_ID: {"$in": [bean.url for bean in beans]}}, {K_ID: 1})]
                
    def store_chatters(self, chatters: list[Chatter]):
        if not chatters: return chatters
        res = self.chatterstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True, exclude_defaults=True) for item in chatters])
        return len(res.inserted_ids or [])

    def update_bean_fields(self, beans: list[Bean], fields: list[str]):
        if not beans: return 0
        create_update = lambda field_values: {
            "$set": {k:v for k,v in field_values.items() if v},
            "$unset": {k:None for k,v in field_values.items() if not v}
        }
        updates = list(map(
            lambda bean: UpdateOne(
                filter = {K_ID: bean.url},
                update = create_update(bean.model_dump(include=fields))
            ),
            beans
        ))
        return self.beanstore.bulk_write(updates, ordered=False, bypass_document_validation=True).matched_count

    def update_beans(self, updates: list[UpdateOne|UpdateMany]):
        if not updates: return 0
        return self.beanstore.bulk_write(updates, ordered=False, bypass_document_validation=True).matched_count

    def delete_old(self, window: int):
        time_filter = {K_UPDATED: { "$lt": ndays_ago(window) }}
        return self.beanstore.delete_many(time_filter).deleted_count
        # TODO: add delete for bookmarked bean

    ################################
    ## BEANS RETRIEVAL AND SEARCH ##
    ################################
    def get_bean(self, **kwargs) -> Bean|GeneratedBean|None:
        project = kwargs.pop('project', None)
        item = self.beanstore.find_one(filter=kwargs, projection=project)
        if item: return GeneratedBean(**item) if item.get(K_KIND) == GENERATED else Bean(**item)

    def query_beans(self, filter: dict = None, group_by: str|list[str] = None, sort_by = None, skip: int = 0, limit: int = 0, project: dict = None):
        pipeline = _beans_query_pipeline(filter, group_by=group_by, sort_by=sort_by, skip=skip, limit=limit, project=project, count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def count_beans(self, filter: dict, group_by: str|list[str] = None, limit: int = 0):
        pipeline = _beans_query_pipeline(filter, group_by, sort_by=None, skip=0, limit=limit, project=None, count=True)
        result = self.beanstore.aggregate(pipeline)
        return next(iter(result), {'total_count': 0})['total_count'] if result else 0

    def vector_search_beans(self, 
        embedding: list[float], 
        similarity_score: float = 0, 
        filter: dict = None, 
        group_by: str|list[str] = None,
        sort_by = None,
        skip: int = 0,
        limit: int = 0, 
        project: dict = None
    ) -> list[Bean]:
        pipline = _beans_vector_search_pipeline(embedding, similarity_score, filter, group_by, sort_by, skip, limit, project, count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipline))
    
    def count_vector_search_beans(self, 
        embedding: list[float], 
        similarity_score: float = None, 
        filter: dict = None, 
        group_by: str|list[str] = None,
        limit: int = 0
    ) -> int:
        pipeline = _beans_vector_search_pipeline(embedding, similarity_score, filter, group_by, None, 0, limit, None, True)
        result = next(self.beanstore.aggregate(pipeline), None)
        return result.get('total_count', 0) if result else 0
    
    def text_search_beans(self, 
        query: str, 
        filter: dict = None,
        group_by: str|list[str] = None,
        sort_by = None, 
        skip: int = 0, 
        limit: int = 0, 
        project: dict = None
    ):
        pipeline = _beans_text_search_pipeline(query, filter=filter, group_by=group_by, sort_by=sort_by, skip=skip, limit=limit, project=project, count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def count_text_search_beans(self, 
        query: str, 
        filter: dict = None,
        group_by: str|list[str] = None,
        limit: int = 0
    ):
        pipeline = _beans_text_search_pipeline(query, filter=filter, group_by=group_by, sort_by=None, skip=0, limit=limit, project=None, count=True)
        result = self.beanstore.aggregate(pipeline)
        return next(iter(result), {'total_count': 0})['total_count'] if result else 0
    
    def sample_beans(self, filter: dict = None, sort_by = None, limit: int = 1, project = None) -> list[Bean]:
        pipeline = [
            { 
                "$match": filter 
            },
            { 
                "$sample": {"size": limit} 
            }
        ]
        if sort_by: pipeline.append({"$sort": sort_by})
        if project: pipeline.append({"$project": project})
        return _deserialize_beans(self.beanstore.aggregate(pipeline=pipeline))

    def query_beans_in_cluster(self, 
        url: str, 
        filter: dict = None, 
        sort_by = None, 
        skip: int = 0,
        limit: int = 0, 
        project: dict = None
    ) -> list[Bean]:
        pipeline = _related_beans_pipeline(url, filter, sort_by, skip, limit, project, False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))

    def count_beans_in_cluster(self, 
        url: str, 
        filter: dict = None, 
        limit: int = 0
    ) -> int:
        pipeline = _related_beans_pipeline(url, filter, None, None, limit, None, True)
        result = next(self.beanstore.aggregate(pipeline), None)
        return result.get('total_count', 0) if result else 0
    
    def _find_bean_for_similar_bean(self, url: str):
        bean = self.beanstore.find_one(
            {
                K_URL: url, 
                K_EMBEDDING: {"$exists": True},
                K_TAGS: {"$exists": True}
            }, 
            projection = {K_TAGS: 1, K_EMBEDDING: 1}
        )
        if not bean: return (None, None)
        similar_filter = {
            K_URL: {"$ne": url},
            K_TAGS: {"$in": bean[K_TAGS]}
        }
        return bean[K_EMBEDDING], similar_filter

    def vector_search_similar_beans(self,
        url: str, 
        similarity_score: float = 0, 
        filter: dict = None, 
        group_by: str|list[str] = None, 
        skip: int = 0, 
        limit: int = 0, 
        project: dict = None
    ):
        emb, sim_fil = self._find_bean_for_similar_bean(url)
        if not emb: return
        if filter: sim_fil.update(filter)
        return self.vector_search_beans(emb, similarity_score, sim_fil, group_by, None, skip, limit, project)
    
    def count_vector_search_similar_beans(self,
        url: str, 
        similarity_score: float = 0,
        filter: dict = None, 
        group_by: str|list[str] = None, 
        limit: int = 0
    ):
        emb, sim_fil = self._find_bean_for_similar_bean(url)
        if not emb: return
        if filter: sim_fil.update(filter)
        return self.count_vector_search_beans(emb, similarity_score, sim_fil, group_by, limit)
    
    def query_tags(self, bean_filter: dict = None, tag_field: str = K_TAGS, remove_tags: list[str] = None, skip: int = 0, limit: int = 0):
        filter = {K_TAGS: {"$exists": True}}
        if bean_filter: filter.update(bean_filter)
        # flatten the tags within the filter
        # take the ones that show up the most
        # sort by the number of times the tags appear
        pipeline = [
            { "$match": filter },
            { "$unwind": f"${tag_field}" },
            {
                "$group": {
                    "_id": f"${tag_field}",
                    K_TRENDSCORE: { "$sum": 1 }
                }
            }         
        ]
        if remove_tags: pipeline.append({"$match": {"_id": {"$nin": remove_tags} if isinstance(remove_tags, list) else {"$ne": remove_tags}}})
        pipeline.append({"$sort": _BY_TRENDSCORE})
        if skip: pipeline.append({"$skip": skip})    
        if limit: pipeline.append({"$limit": limit})   
        return [item[K_ID] for item in self.beanstore.aggregate(pipeline=pipeline)]

    def vector_search_tags(self, 
        bean_embedding: list[float], 
        bean_similarity_score: float = 0, 
        bean_filter: dict = None, 
        tag_field: str = K_TAGS,
        remove_tags: list[str] = None,
        skip: int = 0,
        limit: int = 0
    ) -> list[str]:

        filter = {K_TAGS: {"$exists": True}}
        if bean_filter: filter.update(bean_filter)
        pipeline = [            
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": bean_embedding,
                        "path":   K_EMBEDDING,
                        "filter": filter,
                        "k":      skip+limit if limit else DEFAULT_VECTOR_SEARCH_LIMIT,
                    },
                    "returnStoredSource": True
                }
            },
            {
                "$addFields": { 
                    "search_score": {"$meta": "searchScore"} 
                }
            }
        ]
        if bean_similarity_score: pipeline.append(
            {
                "$match": { 
                    "search_score": {"$gte": bean_similarity_score or DEFAULT_VECTOR_SEARCH_SCORE} 
                }
            }
        )
        pipeline.extend(
            [
                { "$unwind": f"${tag_field}" },
                {
                    "$group": {
                        K_ID: f"${tag_field}",
                        K_TRENDSCORE: { "$sum": 1 },
                    }
                }            
            ]
        )
        if remove_tags: pipeline.append({"$match": {"_id": {"$nin": remove_tags} if isinstance(remove_tags, list) else {"$ne": remove_tags}}})
        pipeline.append({"$sort": _BY_TRENDSCORE})
        if skip: pipeline.append({"$skip": skip})
        if limit: pipeline.append({"$limit": limit})
        return [item[K_ID] for item in self.beanstore.aggregate(pipeline=pipeline)]

    def get_cluster_sizes(self, urls: list[str]) -> list:
        pipeline = [            
            {
                "$group": {
                    "_id": "$cluster_id",
                    "cluster_id": {"$first": "$cluster_id"},            
                    "urls": {"$addToSet": "$url"},
                    "count": {"$sum": 1}
                }        
            },
            {
                "$unwind": "$urls"
            },
            {
                "$match": {"urls": {"$in": urls}}
            },            
            {
                "$project": {
                    "_id": 0,
                    "cluster_id": "$cluster_id",
                    "url": "$urls",
                    "cluster_size": "$count"
                }
            }
        ]    
        return [item for item in self.beanstore.aggregate(pipeline)]
        
    def get_latest_chatters(self, urls: str|list[str] = None) -> list[ChatterAnalysis]:
        """Retrieves the latest social media status from different mediums."""
        current_pipeline = self._chatters_pipeline(urls)
        current_chatters = {item[K_ID]: ChatterAnalysis(**item) for item in self.chatterstore.aggregate(current_pipeline)}
        yesterdays_pipeline = self._chatters_pipeline(urls, 1)
        yesterdays_chatters = {item[K_ID]: ChatterAnalysis(**item) for item in self.chatterstore.aggregate(yesterdays_pipeline)}
        for url, current in current_chatters.items():
            yesterday = yesterdays_chatters.get(url, ChatterAnalysis(url=url))
            current.likes_change = current.likes - yesterday.likes
            current.comments_change = current.comments - yesterday.comments
            current.shares_change = current.shares - yesterday.shares
        return list(current_chatters.values())

    # BUG: if rss feed readers/sites have comments and for those, shares are double counted
    def _chatters_pipeline(self, urls: list[str], days_delta: int = 0):
        pipeline = [
            {
                "$group": {
                    K_ID: {
                        "url": "$url",
                        "chatter_url": "$chatter_url"
                    },
                    K_URL:           {"$first": "$url"},
                    K_LIKES:         {"$max": "$likes"},
                    K_COMMENTS:      {"$max": "$comments"},
                    K_CHATTER_URL:   {"$first": "$chatter_url"},
                    K_COLLECTED:     {"$max": "$collected"},
                }
            },
            {
                "$group": {
                    K_ID:            "$url",
                    K_URL:           {"$first": "$url"},
                    K_LIKES:         {"$sum": "$likes"},
                    K_COMMENTS:      {"$sum": "$comments"},
                    K_SHARES:        {"$sum": 1},
                    K_SHARED_IN:     {"$addToSet": "$chatter_url"},
                    K_COLLECTED:     {"$max": "$collected"}
                }
            }
        ]
        filter = {}
        if urls: filter[K_URL] = field_value(urls)
        if days_delta: filter[K_COLLECTED] = {"$lt": ndays_ago(days_delta)}
        if filter: pipeline = [ {"$match": filter} ] + pipeline
        return pipeline
    
    ##############################
    ## REGISTERED USER FUNCTION ##
    ##############################
    def get_user(self, email: str, linked_account: str = None) -> User|None:
        user = self.userstore.find_one({"email": email})
        if user:
            if linked_account and linked_account not in user["linked_accounts"]:
                self.link_account(email, linked_account)
            return User(**user)
        
    def create_user(self, userinfo: dict, following_baristas: list[str] = None):
        user = User(
            id=userinfo["email"], 
            email=userinfo["email"], 
            name=userinfo["name"], 
            image_url=userinfo.get("picture"), 
            created=datetime.now(),
            updated=datetime.now(),
            linked_accounts=[userinfo["iss"]],
            following=following_baristas
        )
        self.userstore.insert_one(user.model_dump(exclude_none=True, by_alias=True))
        return user

    def link_account(self, email: str, account: str):
        self.userstore.update_one(
            {"email": email}, 
            {
                "$addToSet": {"linked_accounts": account}
            }
        )

    def delete_user(self, email: str):
        self.userstore.delete_one({"_id": email})
    
    def bookmark(self, user: User, url: str):
        return self.pagestore.update_one(
            filter = {K_ID: user.email}, 
            update = { 
                "$addToSet": { "urls": url },
                "$setOnInsert": { 
                    K_OWNER: user.email,
                    K_TITLE: user.name,
                    K_DESCRIPTION: "News, blogs and posts shared by " + user.name
                }
            },
            upsert = True
        ).acknowledged
    
    def unbookmark(self, user: User, url: str):
        return self.pagestore.update_one(
            filter = {K_ID: user.email}, 
            update = { "$pull": { "urls": url } }
        ).acknowledged
    
    def is_bookmarked(self, user: User, url: str):
        return self.pagestore.find_one({K_ID: user.email, "urls": url})

    ###########################
    ## STORED PAGE FUNCTIONS ##
    ###########################
    def follow_page(self, email: str, barista_id: str):
        self.userstore.update_one(
            {"email": email}, 
            {
                "$addToSet": {"following": barista_id}
            }
        )
        return self.userstore.find_one({"email": email})["following"]

    def unfollow_page(self, email: str, barista_id: str):
        self.userstore.update_one(
            {"email": email}, 
            {
                "$pull": {"following": barista_id}
            }
        )
        return self.userstore.find_one({"email": email})["following"]

    def get_page(self, id: str, project=None) -> Page:
        if not id: return
        page = self.pagestore.find_one({K_ID: id}, projection=project)
        if page: return Page(**page)

    def get_pages(self, ids: list[str], project: dict = None):
        if not ids: return
        filter = {K_ID: {"$in": ids}} if ids else {}
        return [Page(**barista) for barista in self.pagestore.find(filter, sort={K_TITLE: 1}, projection=project)]
    
    def sample_pages(self, limit: int, project: dict = None):
        pipeline = [
            { "$match": {"public": True} },
            { "$sample": {"size": limit} }
        ]
        if project: pipeline.append({ "$project": project })
        return [Page(**barista) for barista in self.pagestore.aggregate(pipeline)]
    
    def get_related_pages(self, page_id: str, project=None):
        pipeline = [
            {
                "$match": {K_ID: page_id}
            },
            {
                "$lookup": {
                    "from": PAGES,
                    "localField": K_RELATED,
                    "foreignField": K_ID,
                    "as": K_RELATED
                }
            },
            {
                "$unwind": "$related"
            },        
            {
                "$replaceRoot": { "newRoot": "$related" }
            },
            {
                "$sort": {K_TITLE: 1}
            }
        ]
        if project: pipeline.append({"$project": project})
        return [Page(**page) for page in self.pagestore.aggregate(pipeline)]
     
    def get_following_pages(self, user: User, project=None):
        pipeline = [
            {
                "$match": {K_ID: user.email}
            },
            {
                "$lookup": {
                    "from": PAGES,
                    "localField": K_FOLLOWING,
                    "foreignField": K_ID,
                    "as": K_FOLLOWING
                }
            },
            {
                "$unwind": "$following"
            },        
            {
                "$replaceRoot": { "newRoot": "$following" }
            },
            {
                "$sort": {K_TITLE: 1}
            }
        ]
        if project: pipeline.append({"$project": project})
        return [Page(**barista) for barista in self.userstore.aggregate(pipeline)]

    def search_pages(self, query: str|list[str], project=None):
        pipeline = [
            {   "$match": {"$text": {"$search": query if isinstance(query, str) else " ".join(query)}} },            
            {   "$addFields":  { "search_score": {"$meta": "textScore"}} },
            {   "$project": {"embedding": 0} },
            {   "$sort": {"search_score": -1} },
            {   "$limit": 10 }     
        ]        
        if project: pipeline.append({"$project": project})
        return [Page(**barista) for barista in self.pagestore.aggregate(pipeline)]
    
    def publish(self, id: str):
        return self.pagestore.update_one(
            {K_ID: id}, 
            { "$set": { "public": True } }
        ).acknowledged
        
    def unpublish(self, id: str):
        return self.pagestore.update_one(
            {K_ID: id}, 
            { "$set": { "public": False } }
        ).acknowledged
        
    def is_published(self, id: str):
        val = self.pagestore.find_one({K_ID: id}, {"public": 1, K_OWNER: 1})
        return val.get("public", val[K_OWNER] == SYSTEM) if val else False        
 

## local utilities for pymongo
def _deserialize_beans(cursor) -> list[Bean]:
    try:
        return [Bean(**item) for item in cursor]
    except:
        log.error("failed deserializing beans")
        return []

def _deserialize_chatters(cursor) -> list[Chatter]:
    try:
        return [Chatter(**item) for item in cursor]
    except:
        log.error("failed deserializing chatters")
        return []
    
def updated_in(last_ndays: int):
    return {K_UPDATED: {"$gte": ndays_ago(last_ndays)}}

def created_in(last_ndays: int):
    return {K_CREATED: {"$gte": ndays_ago(last_ndays)}}
