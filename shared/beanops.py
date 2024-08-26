from icecream import ic
from pybeansack.beansack import *
from pybeansack.datamodels import *
from .config import *
from cachetools import TTLCache, cached

beansack: Beansack = None
PROJECTION = {K_EMBEDDING: 0, K_TEXT:0}

def initiatize(db_conn, embedder):
    global beansack
    beansack=Beansack(db_conn, embedder)

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK))
def get_sources():
    return beansack.beanstore.distinct(K_SOURCE)

@cached(TTLCache(maxsize=1, ttl=ONE_WEEK))
def get_content_types():
    return beansack.beanstore.distinct(K_KIND)

# @cached(TTLCache(maxsize=CACHE_SIZE, ttl=FOUR_HOURS))
def trending(query: str|tuple[str], categories: str|tuple[str], tags: str|tuple[str], kind: str|tuple[str], last_ndays: int, start_index: int, topn: int):
    """Retrieves the trending news articles, social media posts, blog articles that match user interest, topic or query."""
    filter=_create_filter(categories, tags, kind, last_ndays)
    sort_by = TRENDING if kind and (POST in kind) else NEWEST_AND_TRENDING
    if query:
        # return beansack.vector_search_beans(query=query, filter=filter, sort_by=TRENDING_AND_LATEST, limit=topn, projection=PROJECTION)
        return beansack.text_search_beans(query=query, filter=filter, sort_by=sort_by, skip=start_index, limit=topn, projection=PROJECTION)
    else:
        return beansack.query_unique_beans(filter=filter, sort_by=sort_by, skip=start_index, limit=topn)
    
@cached(TTLCache(maxsize=CACHE_SIZE, ttl=FOUR_HOURS))
def trending_tags(categories: str|tuple[str], kind: str|tuple[str], last_ndays: int, topn: int) -> list[Bean]:
    return beansack.query_top_tags(filter=_create_filter(categories, None, kind, last_ndays), limit=topn)
    
# @cached(TTLCache(maxsize=CACHE_SIZE, ttl=FOUR_HOURS))
def search(query: str|tuple[str], categories: str|tuple[str], tags: str|tuple[str], kind: str|tuple[str], last_ndays: int, start_index: int, topn: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter=_create_filter(categories, tags, kind, last_ndays)
    if query:
        # return beansack.vector_search_beans(query=query, filter=filter, sort_by=LATEST, limit=topn)
        return beansack.text_search_beans(query=query, filter=filter, sort_by=LATEST, skip=start_index, limit=topn, projection=PROJECTION)
    else:
        return beansack.query_unique_beans(filter=filter, sort_by=LATEST, skip=start_index, limit=topn)
    
@cached(TTLCache(maxsize=CACHE_SIZE, ttl=FOUR_HOURS))
def count_beans(query: str|tuple[str], categories: str|tuple[str], tags: str|tuple[str], kind: str|tuple[str], last_ndays: int, topn: int) -> int:
    filter = _create_filter(categories, tags, kind, last_ndays)
    if query:
        # return beansack.count_vector_search_beans(query=query, filter=filter, limit=topn)
        return beansack.count_text_search_beans(query=query, filter=filter, limit=topn)
    else:
        return beansack.count_unique_beans(filter=filter, limit=topn)

# @cached(TTLCache(maxsize=CACHE_SIZE, ttl=FOUR_HOURS))
def related(cluster_id: str, url: str, last_ndays: int, topn: int):
    filter = _create_filter(None, None, None, last_ndays)
    filter.update({
        K_URL: {"$ne": url}, 
        K_CLUSTER_ID: cluster_id
    })
    return beansack.get_beans(filter=filter, limit=topn, sort_by=NEWEST_AND_TRENDING, projection=PROJECTION)

@cached(TTLCache(maxsize=CACHE_SIZE, ttl=FOUR_HOURS))
def count_related(cluster_id: str, url: str, last_ndays: int, topn: int) -> int:
    filter = _create_filter(None, None, None, last_ndays)
    filter.update({
        K_URL: {"$ne": url}, 
        K_CLUSTER_ID: cluster_id
    })
    return beansack.beanstore.count_documents(filter=filter, limit=topn)
    
def _create_filter(categories: str|tuple[str], tags: str|tuple[str], kind: str|tuple[str], last_ndays: int):
    filter = {}
    if last_ndays:
        filter.update(timewindow_filter(last_ndays))
    if tags:
        # TODO: update with elemMatch regex
        filter.update({K_TAGS: {"$in": [tags] if isinstance(tags, str) else list(tags)}})
    if kind:
        filter.update({K_KIND: {"$in": [kind] if isinstance(kind, str) else list(kind)}})
    if categories == UNCATEGORIZED:
        filter.update({
            "$or": [
                {K_CATEGORIES: {"$exists": False}},
                {K_CATEGORIES: None}
            ]
        })
    elif categories:
        # TODO: update with elemMatch regex
        filter.update({K_CATEGORIES: {"$in": [categories] if isinstance(categories, str) else list(categories)}})
    return filter
