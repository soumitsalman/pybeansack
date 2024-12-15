############################
## BEANSACK DB OPERATIONS ##
############################

from datetime import datetime, timedelta
from functools import reduce
import logging
import operator
from bson import SON
from icecream import ic
from .embedding import *
from .datamodels import *
from .utils import *
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.collection import Collection

TIMEOUT = 300000 # 3 mins

# names of db and collections
BEANSACK = "beansack"
BEANS = "beans"
CHATTERS = "chatters"
SOURCES = "sources"

CLUSTER_GROUP = {
    K_ID: "$cluster_id",
    K_CLUSTER_ID: {"$first": "$cluster_id"},
    K_URL: {"$first": "$url"},
    K_TITLE: {"$first": "$title"},
    K_SUMMARY: {"$first": "$summary"},
    K_HIGHLIGHTS: {"$first": "$highlights"},
    K_TAGS: {"$first": "$tags"},
    K_CATEGORIES: {"$first": "$categories"},
    K_SOURCE: {"$first": "$source"},
    K_CHANNEL: {"$first": "$channel"},
    K_UPDATED: {"$first": "$updated"},
    K_CREATED: {"$first": "$created"},
    K_COLLECTED: {"$first": "$collected"},
    K_LIKES: {"$first": "$likes"},
    K_COMMENTS: {"$first": "$comments"},
    K_SHARES: {"$first": "$shares"},
    K_SEARCH_SCORE: {"$first": "$search_score"},
    K_TRENDSCORE: {"$first": "$trend_score"},
    K_AUTHOR: {"$first": "$author"},
    K_KIND: {"$first": "$kind"},
    K_IMAGEURL: {"$first": "$image_url"}
}

TRENDING = {K_TRENDSCORE: -1}
LATEST = {K_UPDATED: -1}
NEWEST = {K_CREATED: -1}
NEWEST_AND_TRENDING = SON([(K_CREATED, -1), (K_TRENDSCORE, -1)])
LATEST_AND_TRENDING = SON([(K_UPDATED, -1), (K_TRENDSCORE, -1)])

class Beansack:
    beanstore: Collection
    chatterstore: Collection
    sourcestore: Collection
    embedder: Embeddings    

    def __init__(self, conn_str: str, embedder: Embeddings = None):   
             
        client = MongoClient(
            conn_str, 
            timeoutMS=TIMEOUT,
            serverSelectionTimeoutMS=TIMEOUT,
            socketTimeoutMS=TIMEOUT,
            connectTimeoutMS=TIMEOUT,
            retryWrites=True,
            minPoolSize=10,
            maxPoolSize=100)        
        self.beanstore: Collection = client[BEANSACK][BEANS]
        self.chatterstore: Collection = client[BEANSACK][CHATTERS]        
        self.sourcestore: Collection = client[BEANSACK][SOURCES]  
        self.embedder: Embeddings = embedder

    ##########################
    ## STORING AND INDEXING ##
    ##########################
    def store_beans(self, beans: list[Bean]) -> int:   
        beans = self.index_beans(self.not_exists(beans)) 
        if beans:
            res = self.beanstore.insert_many([bean.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for bean in beans], ordered=False)            
            return len(res.inserted_ids)
        return 0

    def not_exists(self, beans: list[Bean]):
        if beans:
            exists = [item[K_URL] for item in self.beanstore.find({K_URL: {"$in": [bean.url for bean in beans]}}, {K_URL: 1})]
            return list({bean.url: bean for bean in beans if (bean.url not in exists)}.values())
                
    # this function checks for embeddings, updated time and any other rectification needed before inserting
    def index_beans(self, beans: list[Bean|Highlight]):
        # for each item if there is no embedding and create one from the text.
        if beans:
            batch_time = now()
            for bean in beans:
                if not bean.embedding and self.embedder:
                    bean.embedding = self.embedder.embed(bean.digest())
                if not bean.updated:
                    bean.updated = batch_time
            return beans

    def store_chatters(self, chatters: list[Chatter]):
        if chatters:
            res = self.chatterstore.insert_many([item.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for item in chatters])
            return len(res.inserted_ids or [])

    # TODO: enable later. this is temporarily disabled
    # def update_beans(self, urls: list[str|list[str]], updates: list[dict]) -> int:
    #     # if update is a single dict then it will apply to all beans with the specified urls
    #     # or else update is a list of equal length, and we will do a bulk_write of update one
    #     if len(urls) != len(updates):
    #         logger.warning("Bulk update discrepency: len(urls) [%d] != len(updates) [%d]", len(urls), len(updates))
        
    #     makeupdate = lambda filter, set_fields: UpdateOne({K_URL: filter}, set_fields) if isinstance(filter, str) else UpdateMany({K_URL: {"$in": filter}}, set_fields)       
    #     writes = list(map(makeupdate, urls, [{"$set": fields} for fields in updates]))
    #     return self.beanstore.bulk_write(writes).modified_count
      
    def delete_old(self, window: int):
        time_filter = {K_UPDATED: { "$lt": ndays_ago(window) }}
        bean_count = self.beanstore.delete_many(time_filter).deleted_count
        chatter_count = self.chatterstore.delete_many(time_filter).deleted_count
        return (bean_count, chatter_count)

    ####################
    ## GET AND SEARCH ##
    ####################

    def get_beans(self, filter, sort_by = None, skip = 0, limit = 0,  projection = None) -> list[Bean]:
        cursor = self.beanstore.find(filter = filter, projection = projection, sort=sort_by, skip = skip, limit=limit)
        return _deserialize_beans(cursor)
    
    def sample_related_beans(self, url: str, filter: dict = None, limit: int = 0) -> list[Bean]:
        if bean := self.beanstore.find_one({K_URL: url}, projection={K_CLUSTER_ID: 1}):
            match_filter = {
                K_URL: {"$ne": url},
                K_CLUSTER_ID: bean[K_CLUSTER_ID]
            }
            if filter:
                match_filter.update(filter)
            pipeline = [
                { "$match": match_filter },
                { "$sample": {"size": limit} },
                { "$sort": NEWEST_AND_TRENDING }
            ]
            return _deserialize_beans(self.beanstore.aggregate(pipeline))
        return []

    def vector_search_beans(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = None, 
            filter = None, 
            sort_by = None,
            skip = None,
            limit = DEFAULT_VECTOR_SEARCH_LIMIT, 
            projection = None
        ) -> list[Bean]:
        pipline = self._vector_search_pipeline(query, embedding, min_score, filter, sort_by, skip, limit, projection)
        return _deserialize_beans(self.beanstore.aggregate(pipeline=pipline))
    
    def count_vector_search_beans(self, query: str = None, embedding: list[float] = None, min_score = DEFAULT_VECTOR_SEARCH_SCORE, filter: dict = None, limit = DEFAULT_VECTOR_SEARCH_LIMIT) -> int:
        pipeline = self._count_vector_search_pipeline(query, embedding, min_score, filter, limit)
        result = list(self.beanstore.aggregate(pipeline))
        return result[0]['total_count'] if result else 0
    
    def text_search_beans(self, query: str, filter = None, sort_by = {K_SEARCH_SCORE: -1}, skip=0, limit=0, projection=None):
        return _deserialize_beans(
            self.beanstore.aggregate(
                self._text_search_pipeline(query, filter=filter, sort_by=sort_by, skip=skip, limit=limit, projection=projection, for_count=False)))
    
    def count_text_search_beans(self, query: str, filter = None, limit = 0):
        result = self.beanstore.aggregate(self._text_search_pipeline(query, filter=filter, sort_by=None, skip=0, limit=limit, projection=None, for_count=True))
        return next(iter(result), {'total_count': 0})['total_count'] if result else 0
    
    def get_unique_beans(self, filter, sort_by = None, skip = 0, limit = 0, projection = None):
        pipeline = self._unique_beans_pipeline(filter, sort_by=sort_by, skip=skip, limit=limit, projection=projection, for_count=False)
        return _deserialize_beans(self.beanstore.aggregate(pipeline))
    
    def count_unique_beans(self, filter, limit = 0):
        pipeline = self._unique_beans_pipeline(filter, sort_by=None, skip=0, limit=limit, projection=None, for_count=True)
        result = self.beanstore.aggregate(pipeline)
        return next(iter(result))['total_count'] if result else 0
    
    def get_tags(self, beans_in_scope, exclude_from_result, skip = 0, limit = 0):
        filter = {K_TAGS: {"$exists": True}}
        if beans_in_scope:
            filter.update(beans_in_scope)
        # Option 1:
        # for the beans in scope
        # take one bean from each cluster for diversification.
        # then for each tag, use an aggregated valuee of latest and trending
        # sort by that sort and then return the list
        # pipeline = [
        #     { "$match": match_filter },
        #     { "$sort": LATEST_AND_TRENDING },
        #     {
        #         # for each cluster tag the tags of the bean with the latest and highest trend 
        #         "$group": {
        #             "_id": "$cluster_id",    
        #             "url": {"$first": "$url"},                
        #             "updated": { "$first": "$updated" },
        #             "trend_score": { "$first": "$trend_score" },
        #             "tags": {"$first": "$tags"}
        #         }
        #     },
        #     { "$unwind": "$tags" },
        #     {
        #         "$group": {
        #             "_id": "$tags",
        #             "tags": {"$first": "$tags"},
        #             "updated": { "$max": "$updated" },
        #             "trend_score": { "$sum": "$trend_score" },
        #             "url": {"$first": "$url"} # this doesn't actually matter. this is just for the sake of datamodel
        #         }
        #     },
        #     { "$sort": LATEST_AND_TRENDING }
        # ]
        # Option 2:
        # flatten the tags within the filter
        # take the ones that show up the most
        # sort by the number of times the tags appear
        pipeline = [
            { "$match": filter },
            { "$unwind": "$tags" },
            {
                "$group": {
                    "_id": "$tags",
                    # "tags": {"$first": "$tags"},
                    "trend_score": { "$sum": 1 },
                    # "url": {"$first": "$url"} # this doesn't actually matter. this is just for the sake of datamodel
                }
            }         
        ]
        if exclude_from_result:
            pipeline.append({"$match": {"_id": {"$nin": exclude_from_result}}})
        pipeline.append({"$sort": TRENDING})
        if skip:
            pipeline.append({"$skip": skip})    
        if limit:
            pipeline.append({"$limit": limit})   
        return [item[K_ID] for item in self.beanstore.aggregate(pipeline=pipeline)]
        # return _deserialize_beans(self.beanstore.aggregate(pipeline))

    def vector_search_tags(self, 
            query: str = None,
            embedding: list[float] = None, 
            min_score = None, 
            beans_in_scope = None, 
            exclude_from_result = None,
            skip = None,
            limit = DEFAULT_VECTOR_SEARCH_LIMIT
        ) -> list[Bean]:

        match_filter = {K_TAGS: {"$exists": True}}
        if beans_in_scope:
            match_filter.update(beans_in_scope)
        pipeline = [            
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": embedding or self.embedder.embed_query(query),
                        "path":   K_EMBEDDING,
                        "filter": match_filter,
                        "k":      DEFAULT_VECTOR_SEARCH_LIMIT,
                    },
                    "returnStoredSource": True
                }
            },
            {
                "$addFields": { "search_score": {"$meta": "searchScore"} }
            },
            {
                "$match": { "search_score": {"$gte": min_score or DEFAULT_VECTOR_SEARCH_SCORE} }
            },
            # NOTE: removing clustering and only keeping the tags that show up the most
            # {
            #     "$sort": LATEST_AND_TRENDING
            # },
            # {
            #     # for each cluster tag the tags of the bean with the latest and highest trend 
            #     "$group": {
            #         "_id": "$cluster_id",    
            #         "url": {"$first": "$url"},                
            #         "updated": { "$first": "$updated" },
            #         "trend_score": { "$first": "$trend_score" },
            #         "tags": {"$first": "$tags"}
            #     }
            # },
            { "$unwind": "$tags" },
            {
                "$group": {
                    K_ID: "$tags",
                    # "tags": {"$first": "$tags"},
                    K_TRENDSCORE: { "$sum": 1 },
                    # "url": {"$first": "$url"} # this doesn't actually matter. this is just for the sake of datamodel
                }
            }            
        ]
        if exclude_from_result:
            pipeline.append({"$match": {K_ID: {"$nin": exclude_from_result}}})
        pipeline.append({"$sort": TRENDING})
        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        return [item[K_ID] for item in self.beanstore.aggregate(pipeline=pipeline)]
        # return _deserialize_beans(self.beanstore.aggregate(pipeline=pipeline))

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
    
    def _unique_beans_pipeline(self, filter, sort_by, skip, limit, projection, for_count):
        pipeline = []
        if filter:
            pipeline.append({"$match": filter})
        if sort_by:
            pipeline.append({"$sort": sort_by})        
        pipeline.append({"$group": CLUSTER_GROUP})
        if sort_by:
            pipeline.append({"$sort": sort_by})
        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        if for_count:
            pipeline.append({"$count": "total_count"})
        if projection:
            pipeline.append({"$project": projection})
        return pipeline

    def _text_search_pipeline(self, text: str, filter, sort_by, skip, limit, projection, for_count):
        match = {"$text": {"$search": text}}
        if filter:
            match.update(filter)

        pipeline = [
            { "$match": match },            
            { "$addFields":  { K_SEARCH_SCORE: {"$meta": "textScore"}} },
            { "$sort": {K_SEARCH_SCORE: -1} }
        ]        
        # means this is for retrieval of the actual contents
        # in this case sort by the what is provided for sorting
        pipeline.append({"$group": CLUSTER_GROUP})
        if sort_by:
            pipeline.append({"$sort": sort_by})           
        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        if for_count:
            pipeline.append({"$count": "total_count"})
        if projection:
            pipeline.append({"$project": projection})
        return pipeline
   
    def _vector_search_pipeline(self, text, embedding, min_score, filter, sort_by, skip, limit, projection):    
        sort_by = sort_by or { "search_score": -1 }
        pipeline = [            
            {
                "$search": {
                    "cosmosSearch": {
                        "vector": embedding or self.embedder.embed_query(text),
                        "path":   K_EMBEDDING,
                        "filter": filter or {},
                        "k":      DEFAULT_VECTOR_SEARCH_LIMIT if limit > 1 else 1, # if limit is 1, then we don't need to search for more than 1
                    }
                }
            },
            {
                "$addFields": { "search_score": {"$meta": "searchScore"} }
            },
            {
                "$match": { "search_score": {"$gte": min_score or DEFAULT_VECTOR_SEARCH_SCORE} }
            },
            {   
                "$sort": sort_by
            },
            {
                "$group": CLUSTER_GROUP
            },
            {   
                "$sort": sort_by
            },
        ]  
        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})
        if projection:
            pipeline.append({"$project": projection})
        return pipeline
    
    def _count_vector_search_pipeline(self, text, embedding, min_score, filter, limit):
        pipline = self._vector_search_pipeline(text, embedding, min_score, filter, None, None, limit, None)
        pipline.append({ "$count": "total_count"})
        return pipline
    
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
                    "_id": {
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

## local utilities for pymongo
def _deserialize_beans(cursor) -> list[Bean]:
    try:
        return [Bean(**item) for item in cursor]
    except:
        _get_logger().error("failed deserializing beans")
        return []

def _deserialize_chatters(cursor) -> list[Chatter]:
    try:
        return [Chatter(**item) for item in cursor]
    except:
        _get_logger().error("failed deserializing chatters")
        return []
    
def _get_logger():
    return logging.getLogger("beansack")

def updated_after(last_ndays: int):
    return {K_UPDATED: {"$gte": ndays_ago(last_ndays)}}

def created_after(last_ndays: int):
    return {K_CREATED: {"$gte": ndays_ago(last_ndays)}}