import re
import humanize
from icecream import ic
import tldextract
from pybeansack.beansack import *
from pybeansack.datamodels import *
from .utils import *
from memoization import cached

beansack: Beansack = None
PROJECTION = {K_EMBEDDING: 0, K_TEXT:0, K_ID: 0}
MAX_LIMIT = 100

def initiatize(db_conn, embedder: Embeddings):
    global beansack
    beansack=Beansack(db_conn, embedder)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def get(urls: str|list[str], tags: str|list[str], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    filter=_create_filter(urls, None, tags, kinds, sources, last_ndays, None, None)
    return beansack.get_beans(filter=filter, sort_by=LATEST_AND_TRENDING, skip=start, limit=limit, projection=PROJECTION)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def embeddings(urls: str|list[str]) -> list[Bean]:
    filter=_create_filter(urls, None, None, None, None, None, None, None)
    return beansack.get_beans(filter=filter, projection={K_EMBEDDING: 1, K_URL: 1})

@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def search(query: str, accuracy: float, tags: str|list[str], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(None, None, tags, kinds, sources, last_ndays, None, None)
    return beansack.vector_search_beans(query=query, min_score=accuracy, filter=filter, sort_by=LATEST_AND_TRENDING, skip=start, limit=limit, projection=PROJECTION)    

@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def unique(tags: str|list[str], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    filter=_create_filter(None, None, tags, kinds, sources, last_ndays, None, None)
    return beansack.get_unique_beans(filter=filter, sort_by=LATEST_AND_TRENDING, skip=start, limit=limit)

# @cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
# def trending(urls: list[str], categories: str|list[str], tags: str|list[str], kinds: str|list[str], last_ndays: int, start: int, limit: int):
#     """Retrieves the trending news articles, social media posts, blog articles that match user interest, topic or query."""
#     ic(categories, tags, kinds, last_ndays, start, limit)
#     filter=_create_filter(urls, categories, tags, kinds, None, last_ndays, start, limit)
#     sort_by = LATEST_AND_TRENDING if kinds and (POST in kinds) else NEWEST_AND_TRENDING
#     # if urls:
#     #     return beansack.get_beans(filter=filter, sort_by=sort_by, skip=start, limit=limit, projection=PROJECTION)    
#     return beansack.get_unique_beans(filter=filter, sort_by=sort_by, skip=start, limit=limit, projection=PROJECTION)

@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def related(url: str, tags: str|list[str], kinds: str|list[str], sources: str|list[str], last_ndays: int, start: int, limit: int):
    bean = beansack.beanstore.find_one({K_URL: url}, projection=PROJECTION)
    if bean:
        filter = _create_filter(None, None, tags, kinds, sources, last_ndays, bean[K_CLUSTER_ID], url)
        return beansack.get_beans(filter=filter, skip=start, limit=limit, sort_by=NEWEST_AND_TRENDING, projection=PROJECTION)

@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def chatters(urls: str|list[str]):
    """Retrieves the latest social media status from different mediums."""
    return beansack.get_chatter_stats(urls)

# @cached(max_size=1, ttl=ONE_WEEK)
def sources():
    return beansack.beanstore.distinct(K_SOURCE)

# @cached(max_size=1, ttl=ONE_DAY)
def tags():
    return beansack.beanstore.distinct(K_TAGS)
    
@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def trending_tags(urls: list[str], categories: str|list[str], kinds: str|list[str], last_ndays: int, start: int, limit: int) -> list[Bean]:
    return beansack.get_trending_tags(filter=_create_filter(urls, categories, None, kinds, None, last_ndays, None, None), skip=start, limit=limit)
    
@cached(max_size=CACHE_SIZE, ttl=ONE_HOUR)
def count_beans(query: str, urls: list[str], categories: str|list[str], tags: str|list[str], kinds: str|list[str], last_ndays: int, limit: int) -> int:
    filter = _create_filter(urls, categories, tags, kinds, None, last_ndays, None, None)
    if query:
        return beansack.count_vector_search_beans(query=query, filter=filter, limit=limit)
    if not categories and not urls:
        return beansack.beanstore.count_documents(filter=filter, limit=limit)
    return beansack.count_unique_beans(filter=filter, limit=limit)

@cached(max_size=CACHE_SIZE, ttl=FOUR_HOURS)
def count_related(cluster_id: str, url: str, limit: int) -> int:
    filter = _create_filter(None, None, None, None, None,None, cluster_id, url)
    return beansack.beanstore.count_documents(filter=filter, limit=limit)
    
def _create_filter(
        urls: list[str], 
        categories: str|list[str], 
        tags: str|list[str], 
        kinds: str|list[str], 
        sources: str|list[str],
        last_ndays: int, 
        cluster_id: str, 
        ignore_url: str) -> dict:
    filter = {
        K_CLUSTER_ID: {"$exists": True},
        K_SUMMARY: {"$exists": True}
    }
    if urls:
        filter[K_URL] = lower_case(urls) 
    if kinds:
        filter[K_KIND] = lower_case(kinds)
    # if categories:
    #     # TODO: remove this
    #     filter[K_CATEGORIES] = lower_case(categories)
    if tags or categories:      
        items = tags if isinstance(tags, list) else [tags]
        items.extend(categories) if isinstance(categories, list) else items.append(categories)
        filter[K_TAGS] = case_insensitive(items)
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
naturalday = lambda date_val: humanize.naturalday(datetime.fromtimestamp(date_val), format="%a, %b %d")