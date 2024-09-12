from icecream import ic
import tldextract
from pybeansack.beansack import *
from pybeansack.datamodels import *
from shared import llmops
from .config import *
from cachetools import TTLCache, cached

beansack: Beansack = None
PROJECTION = {K_EMBEDDING: 0, K_TEXT:0}

def initiatize(db_conn):
    global beansack
    beansack=Beansack(db_conn)

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK))
def get_sources():
    return beansack.beanstore.distinct(K_SOURCE)

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK))
def get_content_types():
    return beansack.beanstore.distinct(K_KIND)

@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_HOUR))
def trending(urls: tuple[str], categories: str|tuple[str], kinds: str|tuple[str], last_ndays: int, start_index: int, topn: int):
    """Retrieves the trending news articles, social media posts, blog articles that match user interest, topic or query."""
    filter=_create_filter(urls, categories, None, kinds, last_ndays)
    sort_by = TRENDING_AND_LATEST if kinds and (POST in kinds) else NEWEST_AND_TRENDING
    if urls:
        return beansack.get_beans(filter=filter, sort_by=sort_by, skip=start_index, limit=topn, projection=PROJECTION)    
    return beansack.query_unique_beans(filter=filter, sort_by=sort_by, skip=start_index, limit=topn)
    
@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_HOUR))
def trending_tags(urls: tuple[str], categories: str|tuple[str], kinds: str|tuple[str], last_ndays: int, topn: int) -> list[Bean]:
    return beansack.query_top_tags(filter=_create_filter(urls, categories, None, kinds, last_ndays), limit=topn)
    
@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_HOUR))
def search(query: str, tags: str|tuple[str], kinds: str|tuple[str], last_ndays: int, start_index: int, topn: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(None, None, tags, kinds, last_ndays)
    if query:
        return beansack.vector_search_beans(embedding=llmops.embed(query), filter=filter, sort_by=TRENDING_AND_LATEST, limit=topn, projection=PROJECTION)
    else:
        return beansack.query_unique_beans(filter=filter, sort_by=TRENDING_AND_LATEST, skip=start_index, limit=topn)
    
@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_HOUR))
def count_beans(query: str, urls: tuple[str], categories: str|tuple[str], tags: str|tuple[str], kind: str|tuple[str], last_ndays: int, topn: int) -> int:
    filter = _create_filter(urls, categories, tags, kind, last_ndays)
    if query:
        return beansack.count_vector_search_beans(embedding=llmops.embed(query), filter=filter, limit=topn)
    if urls:
        return beansack.beanstore.count_documents(filter)
    return beansack.count_unique_beans(filter=filter, limit=topn)

@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_HOUR))
def related(cluster_id: str, url: str, last_ndays: int, topn: int):
    filter = _create_filter(None, None, None, None, last_ndays)
    filter.update({
        K_URL: {"$ne": url}, 
        K_CLUSTER_ID: cluster_id
    })
    return beansack.get_beans(filter=filter, limit=topn, sort_by=NEWEST_AND_TRENDING, projection=PROJECTION)

@cached(TTLCache(maxsize=CACHE_SIZE, ttl=ONE_HOUR))
def count_related(cluster_id: str, url: str, last_ndays: int, topn: int) -> int:
    filter = _create_filter(None, None, None, None, last_ndays)
    filter.update({
        K_URL: {"$ne": url}, 
        K_CLUSTER_ID: cluster_id
    })
    return beansack.beanstore.count_documents(filter=filter, limit=topn)
    
def _create_filter(urls: tuple[str], categories: str|tuple[str], tags: str|tuple[str], kinds: str|tuple[str], last_ndays: int) -> dict:
    filter = {K_CLUSTER_ID: {"$exists": True}}
    if urls:
        filter.update({K_URL: {"$in": list(urls)}})
    if last_ndays:        
        filter.update(created_in(last_ndays) if kinds and ((NEWS in kinds) or (BLOG in kinds)) else updated_in(last_ndays))
    if tags:
        # TODO: update with elemMatch regex
        filter.update({K_TAGS: {"$in": [tags] if isinstance(tags, str) else list(tags)}})
    if kinds:
        filter.update({K_KIND: {"$in": [kinds] if isinstance(kinds, str) else list(kinds)}})
    if categories:
        cat_list_filter = {K_CATEGORIES: {"$in": [categories] if isinstance(categories, str) else list(categories)}}
        # if UNCATEGORIZED in categories:
        #     filter.update({
        #         "$or": [
        #             {K_CATEGORIES: {"$exists": False}},
        #             {K_CATEGORIES: None},
        #             cat_list_filter
        #         ]
        #     })
        # else:
        filter.update(cat_list_filter)
    return filter

favicon = lambda bean: "https://www.google.com/s2/favicons?domain="+tldextract.extract(bean.url).registered_domain