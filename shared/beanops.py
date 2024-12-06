import re
import humanize
from icecream import ic
import tldextract
from pybeansack.mongosack import *
from pybeansack.datamodels import *
from .utils import *
from memoization import cached

beansack: Beansack = None
PROJECTION = {K_EMBEDDING: 0, K_TEXT:0, K_ID: 0}
MAX_LIMIT = 100

def initiatize(db_conn, embedder: Embeddings):
    global beansack
    beansack=Beansack(db_conn, embedder)

def get_kinds():
    return beansack.beanstore.distinct(K_KIND)

# @cached(max_size=1, ttl=ONE_WEEK)
def get_sources():
    return beansack.beanstore.distinct(K_SOURCE)

# @cached(max_size=1, ttl=ONE_DAY)
def get_tags():
    return beansack.beanstore.distinct(K_TAGS)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get_beans(urls: str|list[str]) -> list[Bean]:
    filter={K_URL: {"$in": urls} if isinstance(urls, list) else urls}
    return beansack.get_beans(filter=filter)

def get_bean(url: str) -> Bean:
    return Bean(**beansack.beanstore.find_one(filter={K_URL: url}, projection=PROJECTION))

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def text_search_beans(query: str, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None)
    return beansack.text_search_beans(query=query, filter=filter, skip=start, limit=limit, projection=PROJECTION)    

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def vector_search_beans(query: str, accuracy: float, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None)    
    if is_valid_url(query):
        bean =  beansack.beanstore.find_one(filter={K_URL: query}, projection={K_EMBEDDING: 1, K_URL: 1, K_ID: 0})
        return beansack.vector_search_beans(embedding=bean[K_EMBEDDING], min_score=accuracy, filter=filter, skip=start, limit=limit) if bean else []
    if query:
        return beansack.vector_search_beans(query=query, min_score=accuracy, filter=filter, skip=start, limit=limit) 
    return []

def get_newest_beans(tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None)
    filter.update(created_after(last_ndays))
    return beansack.get_unique_beans(filter=filter, sort_by=NEWEST_AND_TRENDING, skip=start, limit=limit, projection=PROJECTION)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get_trending_beans(tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None)
    return beansack.get_unique_beans(filter=filter, sort_by=LATEST_AND_TRENDING, skip=start, limit=limit, projection=PROJECTION)

@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def get_related(url: str, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    bean = beansack.beanstore.find_one({K_URL: url}, projection={K_CLUSTER_ID: 1})
    if bean:
        filter = _create_filter(tags, kinds, sources, last_ndays, bean[K_CLUSTER_ID], url)
        return beansack.get_beans(filter=filter, skip=start, limit=limit, sort_by=NEWEST_AND_TRENDING, projection=PROJECTION)

def get_chatters(urls: str|list[str]):
    """Retrieves the latest social media status from different mediums."""
    return beansack.get_chatter_stats(urls)
    
@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get_tags_from_trending_beans(tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None)
    return beansack.get_trending_tags(filter=filter, skip=start, limit=limit)

@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get_tags_from_searching_beans(query: str, accuracy: float, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    filter=_create_filter(tags, kinds, sources, last_ndays, None, None)
    # if the query is a valid url, then we use the embedding of the bean to search for it
    if is_valid_url(query):
        bean = beansack.beanstore.find_one(filter={K_URL: query}, projection={K_EMBEDDING: 1, K_URL: 1, K_ID: 0})
        return beansack.vector_search_trending_tags(embedding=bean[K_EMBEDDING], min_score=accuracy, filter=filter, skip=start, limit=limit) if bean else []
    if query:
        return beansack.vector_search_trending_tags(query=query, min_score=accuracy, filter=filter, skip=start, limit=limit)
    return []
    
@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def count_beans(query: str, accuracy: float, tags: str|list[str]|list[list[str]], kinds: str|list[str], sources: str|list[str], last_ndays: int, limit: int) -> int:
    filter = _create_filter(tags, kinds, sources, last_ndays, None, None)
    if is_valid_url(query):
        bean = beansack.beanstore.find_one(filter={K_URL: query}, projection={K_EMBEDDING: 1, K_URL: 1, K_ID: 0})
        return beansack.count_vector_search_beans(embedding=bean[K_EMBEDDING], min_score=accuracy, filter=filter, limit=limit) if bean else 0
    if query:
        return beansack.count_vector_search_beans(query=query, min_score=accuracy, filter=filter, limit=limit)
    return beansack.count_unique_beans(filter=filter, limit=limit)

@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def count_related_beans(cluster_id: str, url: str, limit: int) -> int:
    filter = _create_filter(None, None, None, None, cluster_id, url)
    return beansack.beanstore.count_documents(filter=filter, limit=limit)
    
def _create_filter(
        tags: str|list[str]|list[list[str]], 
        kinds: str|list[str], 
        sources: str|list[str],
        last_ndays: int,
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
    if last_ndays:        
        filter.update(updated_after(last_ndays))
    if cluster_id:
        filter[K_CLUSTER_ID] = cluster_id
    if ignore_url:
        filter[K_URL] = {"$ne": ignore_url}
    return filter

lower_case = lambda items: {"$in": [item.lower() for item in items]} if isinstance(items, list) else items.lower()
case_insensitive = lambda items: {"$in": [re.compile(item, re.IGNORECASE) for item in items]} if isinstance(items, list) else re.compile(items, re.IGNORECASE)
favicon = lambda bean: "https://www.google.com/s2/favicons?domain="+tldextract.extract(bean.url).registered_domain
naturalday = lambda date_val: humanize.naturalday(date_val, format="%a, %b %d")