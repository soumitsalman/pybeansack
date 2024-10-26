import env
from pybeansack.datamodels import *
from pybeansack.embedding import *
from shared import beanops, utils
import logging

# logging.basicConfig(level=logging.WARNING)    
logger: logging.Logger = utils.create_logger("__API__", filename=env.log_file())

def initialize_server():
    embedder = RemoteEmbeddings(env.llm_base_url(), env.llm_api_key(), env.embedder_model(), env.embedder_n_ctx()) \
        if env.llm_base_url() else \
        BeansackEmbeddings(env.embedder_model(), env.embedder_n_ctx())
    beanops.initiatize(env.db_connection_str(), embedder)

def log(function, **kwargs):    
    extra = {"user_id": None, "q": None, "acc": None, "url": None, "tag": None, "kind": None, "source": None, "ndays": None, "start": None, "limit": None, "num_items": None}
    extra.update(kwargs)
    logger.info(function, extra=extra)



from fastapi import FastAPI, HTTPException, Query
from icecream import ic

app = FastAPI(title="Espresso API", version="0.0.1", description="API for Espresso (Alpha)")

@app.get("/beans", response_model=list[Bean]|None)
async def get_beans(
    url: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None),
    tag: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None), 
    source: list[str] = Query(max_length=utils.MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=utils.MIN_WINDOW, le=utils.MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=utils.MIN_LIMIT, le=utils.MAX_LIMIT, default=utils.MAX_LIMIT)):
    """
    Retrieves the bean(s) with the given URL(s).
    """
    res = beanops.get(url, tag, kind, source, ndays, start, limit)  
    log('get_beans', url=url, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_items=len(res) if res else None)
    return res

@app.get("/beans/embeddings", response_model=list[Bean]|None)
async def get_embeddings(url: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None)):
    res = beanops.embeddings(url)
    log('get_embeddings', url=url, num_items=len(res) if res else None)
    return res

@app.get("/beans/search", response_model=list[Bean]|None)
async def search_beans(
    q: str, 
    acc: float = Query(ge=0, le=1, default=utils.DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None), 
    source: list[str] = Query(max_length=utils.MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=utils.MIN_WINDOW, le=utils.MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=utils.MIN_LIMIT, le=utils.MAX_LIMIT, default=utils.DEFAULT_LIMIT)):
    """
    Search beans by various parameters.
    q: query string
    acc: accuracy
    tags: list of tags
    kinds: list of kinds
    source: list of sources
    ndays: last n days
    start: start index
    limit: limit
    """
    res = beanops.search(q, acc, tag, kind, source, ndays, start, limit)
    log('search_beans', q=q, acc=acc, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_items=len(res) if res else None)
    return res

@app.get("/beans/unique", response_model=list[Bean]|None)
async def unique_beans(
    tag: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None),
    kind: list[str] | None = Query(default=None), 
    source: list[str] = Query(max_length=utils.MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=utils.MIN_WINDOW, le=utils.MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=utils.MIN_LIMIT, le=utils.MAX_LIMIT, default=utils.MAX_LIMIT)):
    """
    Retuns a set of unique beans, meaning only one bean from each cluster will be included in the result.
    To retrieve all the beans irrespective of cluster, use /beans endpoint.
    To retrieve the beans related to the beans in this result set, use /beans/related endpoint.
    """
    res = beanops.unique(tag, kind, source, ndays, start, limit)
    log('unique_beans', tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_items=len(res) if res else None)
    return res

@app.get("/beans/related", response_model=list[Bean]|None)
async def get_related_beans(
    url: str, 
    tag: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None),
    kind: list[str] | None = Query(default=None), 
    source: list[str] = Query(max_length=utils.MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=utils.MIN_WINDOW, le=utils.MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=utils.MIN_LIMIT, le=utils.MAX_LIMIT, default=utils.MAX_LIMIT)):
    """
    Retrieves the related beans to the given bean.
    """    
    res = beanops.related(url, tag, kind, source, ndays, start, limit)
    log('get_related_beans', url=url, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_items=len(res) if res else None)
    return res

@app.get("/beans/chatters", response_model=list[Chatter]|None)
async def get_chatters(url: list[str] | None = Query(max_length=utils.MAX_LIMIT, default=None)):
    """
    Retrieves the latest social media stats for the given bean(s).
    """
    res = beanops.chatters(url)
    log('get_chatters', url=url, num_items=len(res) if res else None)
    return res

@app.get("/beans/sources", response_model=list|None)
async def get_sources():
    """
    Retrieves the list of sources.
    """
    res = beanops.sources()
    log('get_sources', num_items=len(res) if res else None)
    return res

@app.get("/beans/tags", response_model=list|None)
async def get_tags():
    """
    Retrieves the list of tags.
    """
    res = beanops.tags()
    log('get_tags', num_items=len(res) if res else None)
    return res



initialize_server()


