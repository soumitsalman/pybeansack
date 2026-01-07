############################
## BEANSACK DB OPERATIONS ##
############################
from concurrent.futures import ThreadPoolExecutor
import os
import logging
import re
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.errors import BulkWriteError
from pymongo.database import Database
from pymongo.collection import Collection
from bson import SON
from .models import *
from .utils import *
from .database import *

log = logging.getLogger(__name__)

TIMEOUT = 300000 # 3 mins
DEFAULT_VECTOR_SEARCH_SCORE = 0.75
DEFAULT_VECTOR_SEARCH_LIMIT = 1000

# names of db and collections
PAGES = "pages"
USERS = "users"

NEWEST = {K_CREATED: -1}
LATEST = {K_CREATED: -1, K_TRENDSCORE: -1}
TRENDING = {K_UPDATED: -1, K_TRENDSCORE: -1}
BY_TRENDSCORE = {K_TRENDSCORE: -1}
BY_SEARCH_SCORE = {K_SEARCH_SCORE: -1}

VALUE_EXISTS = { "$exists": True, "$ne": None}
CLEANUP_WINDOW = 7

class _Bean(Bean):
    id: Optional[str] = Field(default=None, alias="_id")

class _Publisher(Publisher):
    id: Optional[str] = Field(default=None, alias="_id")

field_value = lambda items: {"$in": items} if isinstance(items, list) else items
lower_case = lambda items: {"$in": [item.lower() for item in items if item]} if isinstance(items, list) else items.lower()
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

def _related_beans_pipeline(id, filter, sort_by, skip, limit, project, count):
    related_filter = {K_ID: {"$ne": id}}
    if filter: related_filter.update(filter)

    pipeline = [
        {
            "$match": {
                K_ID: id,
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
        { "$sort": sort_by or BY_SEARCH_SCORE }
    ]   
    if group_by: 
        group_by = [group_by] if isinstance(group_by, str) else group_by
        [pipeline.extend(_create_group_by(gr, sort_by)) for gr in group_by] 
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})    
    return pipeline

_PRIMARY_KEYS = {
    BEANS: K_URL,
    PUBLISHERS: K_SOURCE
}

class MongoDB(Beansack):
    db: Database
    beanstore: Collection
    chatterstore: Collection
    publisherstore: Collection
    userstore: Collection
    pagestore: Collection

    def __init__(self, 
        conn_str: str = os.getenv("MONGO_CONNECTION_STRING", "mongodb://localhost:27017"), 
        db_name: str = os.getenv("MONGO_DATABASE", "beansack")
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
        self.publisherstore: Collection = self.db[PUBLISHERS]  
        self.userstore = self.db[USERS]
        self.pagestore = self.db[PAGES]

    ###################
    ## BEANS STORING ##
    ###################
    def _fix_bean_ids(self, beans: list[Bean]) -> list[_Bean]:
        return [_Bean(id=bean.url, **bean.model_dump(exclude_none=True)) for bean in beans]   

    def exists(self, beans: list[Bean]):
        if not beans: return beans
        return [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]

    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items 
        idkey = _PRIMARY_KEYS[table]    
        get_id = lambda item: getattr(item, idkey)   
        existing_ids = self.db[table].find(
            filter={idkey: {"$in": [get_id(item) for item in items]}}, 
            projection={idkey: 1}
        )
        existing_ids = [item[idkey] for item in existing_ids]
        return list(filter(lambda item: get_id(item) not in existing_ids, items))

    def count_rows(self, table: str, conditions: dict = None) -> int:
        return self.db[table].count_documents(conditions or {})     

    def store_beans(self, beans: list[Bean]) -> int:   
        if not beans: return 0
        beans = prepare_beans_for_store(beans)
        try: return len(self.beanstore.insert_many([bean.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for bean in self._fix_bean_ids(beans)], ordered=False).inserted_ids)
        except BulkWriteError as e: return e.details['nInserted']

    # TODO: split out function for adding embeddings and gists. code commented out below
    # TODO: add function for recompute (for clusters, categories, sentiments and trendscore)
    # TODO: add a recompute and cleanup function
                
    def store_chatters(self, chatters: list[Chatter]):
        if not chatters: return chatters
        chatters = prepare_chatters_for_store(chatters)
        res = self.chatterstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True, exclude_defaults=True) for item in chatters])
        return len(res.inserted_ids or [])

    def store_publishers(self, publishers: list[Publisher]):
        if not publishers: return publishers
        publishers = prepare_publishers_for_store(publishers)
        try: return len(self.publisherstore.insert_many([publisher.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for publisher in self._fix_publisher_ids(publishers)], ordered=False).inserted_ids)
        except BulkWriteError as e: return e.details['nInserted']

    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return 0
        updates = list(map(
            lambda bean: UpdateOne(
                filter = {K_ID: bean.url},
                update = {
                    "$set": bean.model_dump(by_alias=True, exclude_unset=True, exclude_none=True, exclude_defaults=True) \
                        if not columns else \
                        bean.model_dump(by_alias=True, include=columns)
                }
            ),
            beans
        ))
        return self.beanstore.bulk_write(updates, ordered=False, bypass_document_validation=True).matched_count

    def update_embeddings(self, beans: list[Bean]):
        raise NotImplementedError("use update_beans_adhoc instead")

    def delete_old(self, window: int):
        time_filter = {K_UPDATED: { "$lt": ndays_ago(window) }}
        return self.beanstore.delete_many(time_filter).deleted_count
        # TODO: add delete for bookmarked bean

    ################################
    ## BEANS RETRIEVAL AND SEARCH ##
    ################################
    def get_bean(self, **kwargs) -> Bean|None:
        project = kwargs.pop('project', None)
        item = self.beanstore.find_one(filter=kwargs, projection=project)
        if item: return Bean(**item)

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
        id: str, 
        filter: dict = None, 
        sort_by = None, 
        skip: int = 0,
        limit: int = 0, 
        project: dict = None
    ) -> list[Bean]:
        pipeline = _related_beans_pipeline(id, filter, sort_by, skip, limit, project, False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))

    def count_beans_in_cluster(self, 
        id: str, 
        filter: dict = None, 
        limit: int = 0
    ) -> int:
        pipeline = _related_beans_pipeline(id, filter, None, None, limit, None, True)
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
        pipeline.append({"$sort": BY_TRENDSCORE})
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
        pipeline.append({"$sort": BY_TRENDSCORE})
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
    
    def query_chatters(self, collected: datetime, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0): 
        raise NotImplementedError("use lakehouse or ducksack for querying chatters")
        
    def query_aggregated_chatters(self, urls: str|list[str] = None) -> list[Chatter]:
        """Retrieves the latest social media status from different mediums."""
        current_pipeline = self._chatters_pipeline(urls)
        current_chatters = {item[K_ID]: Chatter(**item) for item in self.chatterstore.aggregate(current_pipeline)}
        yesterdays_pipeline = self._chatters_pipeline(urls, 1)
        yesterdays_chatters = {item[K_ID]: Chatter(**item) for item in self.chatterstore.aggregate(yesterdays_pipeline)}
        # for url, current in current_chatters.items():
        #     yesterday = yesterdays_chatters.get(url, Chatter(url=url))
            # current.likes_change = current.likes - yesterday.likes
            # current.comments_change = current.comments - yesterday.comments
            # current.shares_change = current.shares - yesterday.shares
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

    ###########################     
    ## MAINTENANCE FUNCTIONS ##
    ###########################
    def cleanup(self):
        # NOTE: remove anything collected 7 days ago that did not get processed by analyzer
        # TODO: this is a temporary fix.
        cleanup_filter = {
            K_COLLECTED: {"$lt": ndays_ago(CLEANUP_WINDOW)},
            # TODO: remove these later
            K_CLUSTER_ID: {"$exists": False},
            K_GIST: {"$exists": False},
            K_KIND: {"$ne": "AI Generated"}
        }
        count = self.db.beanstore.delete_many(cleanup_filter).deleted_count
        log.debug("cleaned up", extra={"source": "beans", "num_items": count})
        count = self.db.chatterstore.delete_many({K_COLLECTED: {"$lt": ndays_ago(CLEANUP_WINDOW)}}).deleted_count
        log.debug("cleaned up", extra={"source": "chatters", "num_items": count})

    def close(self):
        self.db.client.close()

def _fix_publisher_ids(self, publishers: list[Publisher]) -> list[_Publisher]:
    return [_Publisher(id=publisher.source, **publisher.model_dump(exclude_none=True)) for publisher in publishers]   



## local utilities for pymongo
def _deserialize_beans(cursor) -> list[Bean]:
    try:
        return [Bean(**item) for item in cursor]
    except Exception as e:
        log.error("failed deserializing beans", e)
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


    # def classify_beans(self, beans: list[Bean]) -> list[Bean]:
    #     if not beans: return beans

    #     # these are IO heavy so create thread pools
    #     with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="classify") as exec:
    #         cats = list(exec.map(lambda bean: self.categories.vector_search(bean.embedding, limit=3), beans))
    #         sents = list(exec.map(lambda bean: self.sentiments.vector_search(bean.embedding, limit=3), beans))

    #     # these are simple calculations. threading causes more time loss
    #     updates = list(map(_make_classification_update, beans, cats, sents))
    #     self._push_update(updates, "classified", beans[0].source)
    #     return beans
    
    # find_cluster = lambda self, bean: self.cluster_db.vector_search(embedding=bean.embedding, max_distance=MAX_RELATED_EPS, limit=MAX_RELATED, metric="l2")

    # # current clustering approach
    # # new beans (essentially beans without cluster gets priority for defining cluster)
    # # for each bean without a cluster_id (essentially a new bean) find the related beans within cluster_eps threshold
    # # override their current cluster_id (if any) with the new bean's url   
    # def cluster_beans(self, beans: list[Bean]):
    #     if not beans: return beans

    #     # these are IO heavy so create thread pools
    #     with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="cluster") as exec:
    #         clusters = list(exec.map(self.find_cluster, beans))

    #     # these are simple calculations. threading causes more time loss
    #     updates = chain(*map(_make_cluster_update, beans, clusters))
    #     updates = list({up._filter[K_ID]:up for up in updates}.values())
    #     self._push_update(updates, "clustered", beans[0].source)
    #     return beans

# def _make_update_one(id, update_fields):
#     update_fields = {k:v for k,v in update_fields.items() if v}
#     tags = update_fields.pop(K_TAGS, None)
#     update = {"$set": update_fields}
#     if tags: update["$addToSet"] = {
#         K_TAGS: {
#             "$each": tags if isinstance(tags, list) else [tags]
#         }
#     }
#     return UpdateOne({K_ID: id}, update)

# def _make_cluster_update(bean: Bean, cluster_ids: list[str]): 
#     bean.cluster_id = bean.id
#     bean.related = len(cluster_ids)
#     update_fields = { 
#         K_CLUSTER_ID: bean.cluster_id,
#         K_RELATED: bean.related
#     } 
#     return list(map(_make_update_one, cluster_ids, [update_fields]*len(cluster_ids)))

# def _make_classification_update(bean: Bean, cat: list[str], sent: list[str]): 
#     bean.categories = cat
#     bean.sentiments = sent
#     bean.tags = merge_tags(bean.categories)
#     return _make_update_one(
#         bean.id, 
#         {
#             K_CATEGORIES: bean.categories,
#             K_SENTIMENTS: bean.sentiments,
#             K_TAGS: bean.tags
#         }
#     )    

# def _make_digest_update(bean: Bean, digest: Digest):
#     if not digest: return    
#     bean.regions = digest.regions
#     bean.entities = digest.entities
#     bean.tags = merge_tags(bean.regions, bean.entities)
#     bean.gist = digest.raw
#     return _make_update_one(
#         bean.id,
#         {
#             K_REGIONS: bean.regions,
#             K_ENTITIES: bean.entities,
#             K_TAGS: bean.tags,
#             K_GIST: bean.gist
#         }
#     )

# def run_trend_ranking(self):
    #     # get ranking data from the master db
    #     trends = self.db.get_latest_chatters(None)
    #     for trend in trends:
    #         trend.trend_score = calculate_trend_score(trend)
    #     updates = [UpdateOne(
    #         filter={K_ID: trend.url}, 
    #         update={
    #             "$set": {
    #                 K_LIKES: trend.likes,
    #                 K_COMMENTS: trend.comments,
    #                 K_SHARES: trend.shares,
    #                 K_SHARED_IN: trend.shared_in,
    #                 K_LATEST_LIKES: trend.likes_change,
    #                 K_LATEST_COMMENTS: trend.comments_change,
    #                 K_LATEST_SHARES: trend.shares_change,
    #                 K_TRENDSCORE: trend.trend_score,
    #                 K_UPDATED: trend.collected      
    #             }
    #         }
    #     ) for trend in trends if trend.trend_score] 
    #     count = self.db.update_beans(updates)
    #     log.info("trend ranked", extra={"source": self.run_id, "num_items": count})

    