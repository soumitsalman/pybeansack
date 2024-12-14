import re
from memoization import cached
from icecream import ic
from app.pybeansack.mongosack import *
from app.pybeansack.datamodels import *
from app.shared.utils import *

DEFAULT_ACCURACY = 0.8
DEFAULT_WINDOW = 7
MIN_WINDOW = 1
MAX_WINDOW = 30
DEFAULT_LIMIT = 10
MIN_LIMIT = 1
MAX_LIMIT = 100
DEFAULT_KIND = NEWS
PROJECTION = {K_EMBEDDING: 0, K_TEXT:0, K_ID: 0}

db: Beansack = None

def initiatize(db_conn, embedder: Embeddings):
    global db
    db=Beansack(db_conn, embedder)

@cached(max_size=1, ttl=ONE_WEEK)
def get_all_kinds():
    return db.beanstore.distinct(K_KIND)

def get_all_sources():
    return db.beanstore.distinct(K_SOURCE)

def get_all_tags():
    return db.beanstore.distinct(K_TAGS)

def get_beans(urls: str|list[str], tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None, None)
    if urls:
        filter[K_URL] = {"$in": urls} if isinstance(urls, list) else urls
    return db.get_beans(filter=filter, skip=start, limit=limit)

def get_newest_beans(tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None, None)    
    return db.get_unique_beans(filter=filter, sort_by=NEWEST_AND_TRENDING, skip=start, limit=limit, projection=PROJECTION)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get_trending_beans(tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    filter=_create_filter(tags, kinds, sources, None, last_ndays, None, None)
    return db.get_unique_beans(filter=filter, sort_by=LATEST_AND_TRENDING, skip=start, limit=limit, projection=PROJECTION)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def text_search_beans(query: str, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None, None)
    return db.text_search_beans(query=query, filter=filter, skip=start, limit=limit, projection=PROJECTION)    

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def vector_search_beans(query: str, accuracy: float, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None, None)    
    if is_valid_url(query):
        bean =  db.beanstore.find_one(filter={K_URL: query}, projection={K_EMBEDDING: 1, K_URL: 1, K_ID: 0})
        return db.vector_search_beans(embedding=bean[K_EMBEDDING], min_score=accuracy, filter=filter, skip=start, limit=limit, projection=PROJECTION) if bean else []
    if query:
        return db.vector_search_beans(query=query, min_score=accuracy, filter=filter, skip=start, limit=limit, projection=PROJECTION) 
    return []

@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def get_related(url: str, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    filter = _create_filter(tags, kinds, sources, None, last_ndays, None, None)
    return db.sample_related_beans(url=url, filter=filter, limit=limit)

def get_chatters(urls: str|list[str]):
    """Retrieves the latest social media status from different mediums."""
    return db.get_chatter_stats(urls)
    
@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get_tags(must_have_tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    # searching by updated-after for a wider net
    filter=_create_filter(must_have_tags, kinds, sources, None, last_ndays, None, None)
    return db.get_tags(beans_in_scope=filter, exclude_from_result=must_have_tags, skip=start, limit=limit)

@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def vector_search_tags(query: str, accuracy: float, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    # searching by updated-after for a wider net
    filter=_create_filter(tags, kinds, sources, None, last_ndays, None, None)
    # if the query is a valid url, then we use the embedding of the bean to search for it
    if is_valid_url(query):
        bean = db.beanstore.find_one(filter={K_URL: query}, projection={K_EMBEDDING: 1, K_URL: 1, K_ID: 0})
        return db.vector_search_tags(embedding=bean[K_EMBEDDING], min_score=accuracy, beans_in_scope=filter, exclude_from_result=tags, skip=start, limit=limit) if bean else []
    if query:
        return db.vector_search_tags(query=query, min_score=accuracy, beans_in_scope=filter, exclude_from_result=tags, skip=start, limit=limit)
    return []
    
@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def count_beans(query: str, accuracy: float, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, limit: int) -> int:
    filter = _create_filter(tags, kinds, sources, last_ndays, None, None, None)
    if is_valid_url(query):
        bean = db.beanstore.find_one(filter={K_URL: query}, projection={K_EMBEDDING: 1, K_URL: 1, K_ID: 0})
        return db.count_vector_search_beans(embedding=bean[K_EMBEDDING], min_score=accuracy, filter=filter, limit=limit) if bean else 0
    if query:
        return db.count_vector_search_beans(query=query, min_score=accuracy, filter=filter, limit=limit)
    return db.count_unique_beans(filter=filter, limit=limit)

# @cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
# def count_related_beans(cluster_id: str, url: str, limit: int) -> int:
#     filter = _create_filter(None, None, None, None, cluster_id, url)
#     return db.beanstore.count_documents(filter=filter, limit=limit)
    
def _create_filter(
        tags: str|list[str]|list[list[str]], 
        kinds: str|list[str], 
        sources: str|list[str],
        created_in_last_ndays: int,
        updated_in_last_ndays: int,
        cluster_id: str, 
        ignore_url: str) -> dict:   
    filter = {}
    if kinds:
        filter[K_KIND] = lower_case(kinds)
    if tags: # non-empty or non-null value of tags is important
        if isinstance(tags, str):
            filter[K_TAGS] = tags            
        if isinstance(tags, list) and all(isinstance(tag, str) for tag in tags): # this is an array of strings
            filter[K_TAGS] = {"$in": tags}
        if isinstance(tags, list) and all(isinstance(taglist, list) for taglist in tags): # this is an array of arrays
            filter["$and"] = [{K_TAGS: {"$in": taglist}} for taglist in tags]
    if sources:
        # TODO: make it look into both source field of the beans and the channel field of the chatters
        filter[K_SOURCE] = case_insensitive(sources)
    if created_in_last_ndays:
        filter[K_CREATED] = {"$gte": ndays_ago(created_in_last_ndays)}
    if updated_in_last_ndays: 
        filter[K_UPDATED] = {"$gte": ndays_ago(updated_in_last_ndays)}  
    if cluster_id:
        filter[K_CLUSTER_ID] = cluster_id
    if ignore_url:
        filter[K_URL] = {"$ne": ignore_url}
    return filter

lower_case = lambda items: {"$in": [item.lower() for item in items]} if isinstance(items, list) else items.lower()
case_insensitive = lambda items: {"$in": [re.compile(item, re.IGNORECASE) for item in items]} if isinstance(items, list) else re.compile(items, re.IGNORECASE)
