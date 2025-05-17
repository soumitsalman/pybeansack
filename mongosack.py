############################
## BEANSACK DB OPERATIONS ##
############################
import os
from datetime import datetime, timedelta
import logging
from bson import SON
from icecream import ic
from .models import *
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.collection import Collection

log = logging.getLogger(__name__)

TIMEOUT = 300000 # 3 mins
DEFAULT_VECTOR_SEARCH_SCORE = 0.7
DEFAULT_VECTOR_SEARCH_LIMIT = 1000

# names of db and collections
BEANS = "beans"
CHATTERS = "chatters"
SOURCES = "sources"

# CLUSTER_GROUP = {
#     K_ID: "$cluster_id",
#     K_CLUSTER_ID: {"$first": "$cluster_id"},
#     K_URL: {"$first": "$url"},
#     K_TITLE: {"$first": "$title"},
#     K_SUMMARY: {"$first": "$summary"},
#     K_TAGS: {"$first": "$tags"},
#     K_CATEGORIES: {"$first": "$categories"},
#     K_SOURCE: {"$first": "$source"},
#     K_UPDATED: {"$first": "$updated"},
#     K_CREATED: {"$first": "$created"},
#     K_LIKES: {"$first": "$likes"},
#     K_COMMENTS: {"$first": "$comments"},
#     K_SHARES: {"$first": "$shares"},
#     K_SEARCH_SCORE: {"$first": "$search_score"},
#     K_TRENDSCORE: {"$first": "$trend_score"},
#     K_AUTHOR: {"$first": "$author"},
#     K_KIND: {"$first": "$kind"},
#     K_IMAGEURL: {"$first": "$image_url"}
# }

TRENDING = {K_TRENDSCORE: -1}
LATEST = {K_UPDATED: -1}
NEWEST = {K_CREATED: -1}
NEWEST_AND_TRENDING = SON([(K_CREATED, -1), (K_TRENDSCORE, -1)])
LATEST_AND_TRENDING = SON([(K_UPDATED, -1), (K_TRENDSCORE, -1)])
_BY_SEARCH_SCORE = {K_SEARCH_SCORE: -1}

now = datetime.now
ndays_ago = lambda ndays: now() - timedelta(days=ndays)


group_by = lambda field: [
    {
        "$group": {
            "_id": { field: f"${field}" },
            "doc": { "$first": "$$ROOT" }
        }
    },
    {
        "$replaceRoot": { "newRoot": "$doc" }
    }
]

def _beans_query_pipeline(filter: dict, distinct_field: str, sort_by, skip: int, limit: int, project: dict, count: bool):
    # pipeline = []
    # if filter:
    #     pipeline.append({"$match": filter})
    # if sort_by:
    #     pipeline.append({"$sort": sort_by})        
    # pipeline.append({"$group": CLUSTER_GROUP})
    # if sort_by:
    #     pipeline.append({"$sort": sort_by})
    # if skip:
    #     pipeline.append({"$skip": skip})
    # if limit:
    #     pipeline.append({"$limit": limit})
    # if for_count:
    #     pipeline.append({"$count": "total_count"})
    # if projection:
    #     pipeline.append({"$project": projection})
    # return pipeline
    pipeline = []
    if filter: pipeline.append({"$match": filter})
    if sort_by: pipeline.append({"$sort": sort_by})        
    if distinct_field: pipeline.extend(group_by(distinct_field))
    if sort_by: pipeline.append({"$sort": sort_by})
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})
    return pipeline

def _beans_vector_search_pipeline(embedding: list[float], similarity_score: float, filter: dict, distinct_field: str, sort_by, skip: int, limit: int, project: dict, count: bool):    
    pipeline = [            
        {
            "$search": {
                "cosmosSearch": {
                    "vector": embedding,
                    "path":   K_EMBEDDING,
                    "filter": filter or {},
                    # if there is no group_by then limit the search set else cast a wider net
                    "k":      skip+limit if limit and not distinct_field else DEFAULT_VECTOR_SEARCH_LIMIT,
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
    if distinct_field: pipeline.extend(group_by(distinct_field))
    if sort_by: pipeline.append({"$sort": sort_by})
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})
    return pipeline

def _beans_text_search_pipeline(text: str, filter: dict, distinct_field: str, sort_by, skip: int, limit: int, project: dict, count: bool):
    match = {"$text": {"$search": text}}
    if filter: match.update(filter)
    pipeline = [
        { "$match": match },            
        { "$addFields":  { K_SEARCH_SCORE: {"$meta": "textScore"}} },
        { "$sort": sort_by or _BY_SEARCH_SCORE }
    ]   
    if distinct_field: pipeline.extend(group_by(distinct_field))
    if sort_by: pipeline.append({"$sort": sort_by})
    if skip: pipeline.append({"$skip": skip})
    if limit: pipeline.append({"$limit": limit})
    if project: pipeline.append({"$project": project})
    if count: pipeline.append({"$count": "total_count"})
    
    # # means this is for retrieval of the actual contents
    # # in this case sort by the what is provided for sorting
    # pipeline.append({"$group": CLUSTER_GROUP})
    # if sort_by:
    #     pipeline.append({"$sort": sort_by})           
    # if skip:
    #     pipeline.append({"$skip": skip})
    # if limit:
    #     pipeline.append({"$limit": limit})
    # if count: bool:
    #     pipeline.append({"$count": "total_count"})
    # if project:
    #     pipeline.append({"$project": project})
    return pipeline


class Beansack:
    beanstore: Collection
    chatterstore: Collection
    sourcestore: Collection
    users: Collection
    baristas: Collection

    def __init__(self, 
        conn_str: str = os.getenv("REMOTE_DB_CONNECTION_STRING", "mongodb://localhost:27017"), 
        db_name: str = os.getenv("DB_NAME", "beansack")
    ):  
        client = MongoClient(
            conn_str, 
            timeoutMS=TIMEOUT,
            serverSelectionTimeoutMS=TIMEOUT,
            socketTimeoutMS=TIMEOUT,
            connectTimeoutMS=TIMEOUT,
            retryWrites=True,
            minPoolSize=10,
            maxPoolSize=100)        
        self.beanstore: Collection = client[db_name][BEANS]
        self.chatterstore: Collection = client[db_name][CHATTERS]        
        self.sourcestore: Collection = client[db_name][SOURCES]  
        self.users = client[db_name]["users"]
        self.baristas = client[db_name]["baristas"]

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
        res = self.chatterstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for item in chatters])
        return len(res.inserted_ids or [])

    def update_beans(self, updates: list):
        if not updates: return 0
        return self.beanstore.bulk_write(updates, ordered=False, bypass_document_validation=True).matched_count

    def delete_old(self, window: int):
        time_filter = {K_UPDATED: { "$lt": ndays_ago(window) }}
        return self.beanstore.delete_many(time_filter).deleted_count
        # TODO: add delete for bookmarked bean

    ################################
    ## BEANS RETRIEVAL AND SEARCH ##
    ################################
    def get_bean(self, url: str, project: dict = None) -> Bean|None:
        item = self.beanstore.find_one(filter={K_ID: url}, projection=project)
        if item: return Bean(**item)

    def query_beans(self, filter: dict = None, distinct_field: str = None, sort_by = None, skip: int = 0, limit: int = 0, project: dict = None):
        pipeline = _beans_query_pipeline(filter, distinct_field = distinct_field, sort_by=sort_by, skip=skip, limit=limit, project=project, count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def count_beans(self, filter: dict, distinct_field: str = None, limit: int = 0):
        pipeline = _beans_query_pipeline(filter, distinct_field, sort_by=None, skip=0, limit=limit, project=None, count=True)
        result = self.beanstore.aggregate(pipeline)
        return next(iter(result), {'total_count': 0})['total_count'] if result else 0

    # def vector_search_beans(self, 
    #     embedding: list[float] = None, 
    #     min_score = DEFAULT_VECTOR_SEARCH_SCORE, 
    #     filter = None, 
    #     sort_by = None,
    #     skip = 0,
    #     limit = DEFAULT_VECTOR_SEARCH_LIMIT, 
    #     projection = None
    # ) -> list[Bean]:
    #     pipeline = [            
    #         {
    #             "$search": {
    #                 "cosmosSearch": {
    #                     "vector": embedding,
    #                     "path":   K_EMBEDDING,
    #                     "filter": filter or {},
    #                     "k":      skip+limit,
    #                 }
    #             }
    #         },
    #         {
    #             "$addFields": { "search_score": {"$meta": "searchScore"} }
    #         }
    #     ]  
    #     if min_score: pipeline.append({"$match": { "search_score": {"$gte": min_score}}})
    #     if sort_by: pipeline.append({"$sort": sort_by})
    #     if skip: pipeline.append({"$skip": skip})
    #     if limit: pipeline.append({"$limit": limit})
    #     if projection: pipeline.append({"$project": projection})
    #     return _deserialize_beans(self.beanstore.aggregate(pipeline=pipeline))

    def vector_search_beans(self, 
        embedding: list[float], 
        similarity_score: float = 0, 
        filter: dict = None, 
        distinct_field: str = None,
        sort_by = None,
        skip: int = 0,
        limit: int = 0, 
        project: dict = None
    ) -> list[Bean]:
        pipline = _beans_vector_search_pipeline(embedding, similarity_score, filter, distinct_field, sort_by, skip, limit, project, count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipline))
    
    def count_vector_search_beans(self, 
        embedding: list[float], 
        similarity_score: float = None, 
        filter: dict = None, 
        distinct_field: str = None,
        limit: int = 0
    ) -> int:
        pipeline = _beans_vector_search_pipeline(embedding, similarity_score, filter, distinct_field, None, 0, limit, None, True)
        result = list(self.beanstore.aggregate(pipeline))
        return result[0]['total_count'] if result else 0
    
    def text_search_beans(self, 
        query: str, 
        filter: dict = None,
        distinct_field: str = None,
        sort_by = None, 
        skip: int = 0, 
        limit: int = 0, 
        project: dict = None
    ):
        pipeline = _beans_text_search_pipeline(query, filter=filter, distinct_field=distinct_field, sort_by=sort_by, skip=skip, limit=limit, project=project, ount=False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def count_text_search_beans(self, 
        query: str, 
        filter: dict = None,
        distinct_field: str = None,
        limit: int = 0
    ):
        pipeline = _beans_text_search_pipeline(query, filter=filter, distinct_field=distinct_field, sort_by=None, skip=0, limit=limit, project=None, count=True)
        result = self.beanstore.aggregate(pipeline)
        return next(iter(result), {'total_count': 0})['total_count'] if result else 0
    
    def sample_beans(self, filter: dict = None, sort_by = None, limit: int = 1, projection = None) -> list[Bean]:
        pipeline = [
            { 
                "$match": filter 
            },
            { 
                "$sample": {"size": limit} 
            }
        ]
        if sort_by: pipeline.append({"$sort": sort_by})
        if projection: pipeline.append({"$project": projection})
        return _deserialize_beans(self.beanstore.aggregate(pipeline=pipeline))

    def query_related_beans(self, 
        url: str, 
        filter: dict = None, 
        sort_by = None, 
        limit: int = 0, 
        project: dict = None
    ) -> list[Bean]:
        pipeline = [
            { 
                "$match": {K_ID: url} 
            },
            {
                "$lookup": {
                    "from": "beans",
                    "localField": "cluster_id",
                    "foreignField": "cluster_id",
                    "as": "related_beans"
                }
            },
            {
                "$unwind": "$related_beans"
            },            
            {
                "$match": {
                    "related_beans._id": {"$ne": url}
                }
            },
            {
                "$replaceRoot": {
                    "newRoot": "$related_beans"
                }
            }
        ]
        if filter: pipeline.append({"$match": filter})
        if limit: pipeline.append({"$sample": {"size": limit}})
        if sort_by: pipeline.append({"$sort": sort_by})
        if limit: pipeline.append({"$limit": limit})
        if project: pipeline.append({"$project": project})
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def vector_search_similar_beans(self,
        bean_url: str, 
        similarity_score: float = 0, 
        filter: dict= None, 
        distinct_field: str = None, 
        skip: int = 0, 
        limit: int = 0, 
        project: dict = None
    ):
        bean = self.beanstore.find_one(
            {
                K_ID: bean_url, 
                K_EMBEDDING: {"$exists": True}
            }, 
            projection = {K_TAGS: 1, K_EMBEDDING: 1}
        )
        if not bean: return
        if K_TAGS in bean: 
            if filter: filter.update({K_TAGS: bean[K_TAGS]})
            else: filter = {K_TAGS: bean[K_TAGS]}
        return self.vector_search_beans(bean[K_EMBEDDING], similarity_score, filter, distinct_field, None, skip, limit, project)
    
    def count_vector_search_similar_beans(self,
        bean_url: str, 
        similarity_score: float = 0, 
        filter: dict= None, 
        distinct_field: str = None, 
        limit: int = 0
    ):
        bean = self.beanstore.find_one(
            {
                K_ID: bean_url, 
                K_EMBEDDING: {"$exists": True}
            }, 
            projection = {K_TAGS: 1, K_EMBEDDING: 1}
        )
        if not bean: return 0
        if K_TAGS in bean: 
            if filter: filter.update({K_TAGS: bean[K_TAGS]})
            else: filter = {K_TAGS: bean[K_TAGS]}
        return self.count_vector_search_beans(bean[K_EMBEDDING], similarity_score, filter, distinct_field, limit)

    
    def query_tags(self, beans_filter: dict, remove_tags: list[str], skip: int = 0, limit: int = 0):
        filter = {K_TAGS: {"$exists": True}}
        if beans_filter: filter.update(beans_filter)
        # flatten the tags within the filter
        # take the ones that show up the most
        # sort by the number of times the tags appear
        pipeline = [
            { "$match": filter },
            { "$unwind": "$tags" },
            {
                "$group": {
                    "_id": "$tags",
                    K_TRENDSCORE: { "$sum": 1 }
                }
            }         
        ]
        if remove_tags: pipeline.append({"$match": {"_id": {"$nin": remove_tags}}})
        pipeline.append({"$sort": TRENDING})
        if skip: pipeline.append({"$skip": skip})    
        if limit: pipeline.append({"$limit": limit})   
        return [item[K_ID] for item in self.beanstore.aggregate(pipeline=pipeline)]
        # return _deserialize_beans(self.beanstore.aggregate(pipeline))

    def vector_search_tags(self, 
        bean_embedding: list[float], 
        bean_similarity_score: float = 0, 
        bean_filter: dict = None, 
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
                { "$unwind": "$tags" },
                {
                    "$group": {
                        K_ID: "$tags",
                        K_TRENDSCORE: { "$sum": 1 },
                    }
                }            
            ]
        )
        if remove_tags: pipeline.append({"$match": {K_ID: {"$nin": remove_tags}}})
        pipeline.append({"$sort": TRENDING})
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
    
    # def _make_distinct_beans_pipeline(self, field, filter, sort_by, skip, limit, project, count):
    #     # pipeline = []
    #     # if filter:
    #     #     pipeline.append({"$match": filter})
    #     # if sort_by:
    #     #     pipeline.append({"$sort": sort_by})        
    #     # pipeline.append({"$group": CLUSTER_GROUP})
    #     # if sort_by:
    #     #     pipeline.append({"$sort": sort_by})
    #     # if skip:
    #     #     pipeline.append({"$skip": skip})
    #     # if limit:
    #     #     pipeline.append({"$limit": limit})
    #     # if for_count:
    #     #     pipeline.append({"$count": "total_count"})
    #     # if projection:
    #     #     pipeline.append({"$project": projection})
    #     # return pipeline
    #     pipeline = []
    #     if filter: pipeline.append({"$match": filter})
    #     if sort_by: pipeline.append({"$sort": sort_by})        
    #     pipeline.extend(group_by(field))
    #     if sort_by: pipeline.append({"$sort": sort_by})
    #     if skip: pipeline.append({"$skip": skip})
    #     if limit: pipeline.append({"$limit": limit})
    #     if project: pipeline.append({"$project": project})
    #     if count: pipeline.append({"$count": "total_count"})
    #     return pipeline

    # def _text_search_pipeline(self, text: str, filter, sort_by, skip, limit, projection, for_count):
    #     match = {"$text": {"$search": text}}
    #     if filter:
    #         match.update(filter)

    #     pipeline = [
    #         { "$match": match },            
    #         { "$addFields":  { K_SEARCH_SCORE: {"$meta": "textScore"}} },
    #         { "$sort": {K_SEARCH_SCORE: -1} }
    #     ]        
    #     # means this is for retrieval of the actual contents
    #     # in this case sort by the what is provided for sorting
    #     pipeline.append({"$group": CLUSTER_GROUP})
    #     if sort_by:
    #         pipeline.append({"$sort": sort_by})           
    #     if skip:
    #         pipeline.append({"$skip": skip})
    #     if limit:
    #         pipeline.append({"$limit": limit})
    #     if for_count:
    #         pipeline.append({"$count": "total_count"})
    #     if projection:
    #         pipeline.append({"$project": projection})
    #     return pipeline
   
    # def _vector_search_pipeline(self, embedding, min_score, filter, sort_by, skip, limit, projection):    
    #     sort_by = sort_by or { "search_score": -1 }
    #     pipeline = [            
    #         {
    #             "$search": {
    #                 "cosmosSearch": {
    #                     "vector": embedding,
    #                     "path":   K_EMBEDDING,
    #                     "filter": filter or {},
    #                     "k":      DEFAULT_VECTOR_SEARCH_LIMIT if limit > 1 else 1, # if limit is 1, then we don't need to search for more than 1
    #                 }
    #             }
    #         },
    #         {
    #             "$addFields": { "search_score": {"$meta": "searchScore"} }
    #         },
    #         {
    #             "$match": { "search_score": {"$gte": min_score or DEFAULT_VECTOR_SEARCH_SCORE} }
    #         },
    #         {   
    #             "$sort": sort_by
    #         },
    #         {
    #             "$group": {
    #                 "_id": { "cluster_id": "$cluster_id" },
    #                 "doc": { "$first": "$$ROOT" }
    #             }
    #         },
    #         {
    #             "$replaceRoot": { "newRoot": "$doc" }
    #         },
    #         {   
    #             "$sort": sort_by
    #         },
    #     ]  
    #     if skip:
    #         pipeline.append({"$skip": skip})
    #     if limit:
    #         pipeline.append({"$limit": limit})
    #     if projection:
    #         pipeline.append({"$project": projection})
    #     return pipeline
    
    # def _count_vector_search_pipeline(self, embedding, min_score, filter, limit):
    #     pipline = self._vector_search_pipeline(embedding, min_score, filter, None, None, limit, None)
    #     pipline.append({ "$count": "total_count"})
    #     return pipline
    
    def get_chatter_stats(self, urls: str|list[str]) -> list[Chatter]:
        """Retrieves the latest social media status from different mediums."""
        pipeline = self._chatter_stats_pipeline(urls)
        return _deserialize_chatters(self.chatterstore.aggregate(pipeline))
        
    def get_consolidated_chatter_stats(self, urls: list[str]) -> list[Chatter]:
        pipeline = self._chatter_stats_pipeline(urls) + [            
            {
                "$group": {
                    "_id":           "$url",
                    "url":           {"$first": "$url"},                    
                    "likes":         {"$sum": "$likes"},
                    "comments":      {"$sum": "$comments"}
                }
            }
        ]
        return _deserialize_chatters(self.chatterstore.aggregate(pipeline))
    
    def _chatter_stats_pipeline(self, urls: list[str]):
        return [
            {
                "$match": { K_URL: {"$in": urls} if isinstance(urls, list) else urls }
            },
            {
                "$sort": {K_UPDATED: -1}
            },
            {
                "$group": {
                    K_ID: {
                        "url": "$url",
                        "container_url": "$container_url"
                    },
                    K_URL:           {"$first": "$url"},
                    K_UPDATED:       {"$first": "$updated"},                
                    K_SOURCE:        {"$first": "$source"},
                    K_CHANNEL:       {"$first": "$channel"},
                    K_CONTAINER_URL: {"$first": "$container_url"},
                    K_LIKES:         {"$first": "$likes"},
                    K_COMMENTS:      {"$first": "$comments"}                
                }
            }
        ]
    
    ##########################
    ## USER AND BARISTA OPS ##
    ##########################
    def get_user(self, email: str, linked_account: str = None) -> User|None:
        user = self.users.find_one({"email": email})
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

    def get_barista(self, id: str) -> Barista:
        barista = self.baristas.find_one({K_ID: id})
        if barista: return Barista(**barista)

    def get_baristas(self, ids: list[str], projection: dict = {K_EMBEDDING: 0}):
        filter = {K_ID: {"$in": ids}} if ids else {}
        return [Barista(**barista) for barista in self.baristas.find(filter, sort={K_TITLE: 1}, projection=projection)]
    
    def sample_baristas(self, limit: int, project: dict):
        pipeline = [
            { "$match": {"public": True} },
            { "$sample": {"size": limit} },
            { "$project": project }
        ]
        return [Barista(**barista) for barista in self.baristas.aggregate(pipeline)]
     
    def get_following_baristas(self, user: User):
        following = self.users.find_one({K_ID: user.email}, {K_FOLLOWING: 1})
        if following:
            return self.get_baristas(following["following"])

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
            {K_ID: barista_id}, 
            { "$set": { "public": True } }
        ).acknowledged
        
    def unpublish(self, barista_id: str):
        return self.baristas.update_one(
            {K_ID: barista_id}, 
            { "$set": { "public": False } }
        ).acknowledged
        
    def is_published(self, barista_id: str):
        val = self.baristas.find_one({K_ID: barista_id}, {"public": 1, K_OWNER: 1})
        return val.get("public", val[K_OWNER] == SYSTEM) if val else False        

    def bookmark(self, user: User, url: str):
        return self.baristas.update_one(
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
        return self.baristas.update_one(
            filter = {K_ID: user.email}, 
            update = { "$pull": { "urls": url } }
        ).acknowledged
    
    def is_bookmarked(self, user: User, url: str):
        return self.baristas.find_one({K_ID: user.email, "urls": url})
    

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
    
def updated_after(last_ndays: int):
    return {K_UPDATED: {"$gte": ndays_ago(last_ndays)}}

def created_after(last_ndays: int):
    return {K_CREATED: {"$gte": ndays_ago(last_ndays)}}
