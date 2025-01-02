import app.shared.env as env
import logging
from azure.monitor.opentelemetry import configure_azure_monitor

configure_azure_monitor(
    connection_string=env.APPINSIGHTS_CONNECTION_STRING, 
    logger_name=env.APP_NAME, 
    instrumentation_options={"fastapi": {"enabled": True}})  
logger: logging.Logger = logging.getLogger(env.APP_NAME)

from pybeansack.datamodels import *
from pybeansack.embedding import *
from shared.beanops import *
from shared.utils import *
from fastapi import FastAPI, Query
from icecream import ic

app = FastAPI(title=env.APP_NAME, version="0.0.1", description="API for Espresso (Alpha)")

@app.on_startup
def initialize_server():
    logger.setLevel(logging.INFO)
    embedder = RemoteEmbeddings(env.LLM_BASE_URL, env.LLM_API_KEY, env.EMBEDDER_MODEL, env.EMBEDDER_N_CTX) \
        if env.LLM_BASE_URL else \
        BeansackEmbeddings(env.EMBEDDER_MODEL, env.EMBEDDER_N_CTX)
    initiatize(env.DB_CONNECTION_STRING, embedder)

@app.get("/beans", response_model=list[Bean]|None)
async def get_beans(
    url: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=MAX_LIMIT, default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=MAX_LIMIT)):
    """
    Retrieves the bean(s) with the given URL(s).
    """
    res = get_beans(url, tag, kind, source, ndays, start, limit)  
    log('get_beans', url=url, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/search", response_model=list[Bean]|None)
async def search_beans(
    q: str, 
    acc: float = Query(ge=0, le=1, default=DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=MAX_LIMIT, default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=DEFAULT_LIMIT)):
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
    res = vector_search_beans(q, acc, tag, kind, source, ndays, start, limit)
    log('search_beans', q=q, acc=acc, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/trending", response_model=list[Bean]|None)
async def trending_beans(
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=MAX_LIMIT)):
    """
    Retuns a set of unique beans, meaning only one bean from each cluster will be included in the result.
    To retrieve all the beans irrespective of cluster, use /beans endpoint.
    To retrieve the beans related to the beans in this result set, use /beans/related endpoint.
    """
    res = get_trending_beans(tag, kind, source, ndays, start, limit)
    log('trending_beans', tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/related", response_model=list[Bean]|None)
async def get_related_beans(
    url: str, 
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=MAX_LIMIT)):
    """
    Retrieves the related beans to the given bean.
    """    
    res = get_related(url, tag, kind, source, ndays, start, limit)
    log('get_related_beans', url=url, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

# @app.get("/chatters", response_model=list[Chatter]|None)
# async def get_chatters(url: list[str] | None = Query(max_length=MAX_LIMIT, default=None)):
#     """
#     Retrieves the latest social media stats for the given bean(s).
#     """
#     res = shared.beanops.get_chatters(url)
#     log(logger, 'get_chatters', url=url, num_returned=res)
#     return res

@app.get("/sources/all", response_model=list|None)
async def get_sources():
    """
    Retrieves the list of sources.
    """
    res = get_all_sources()
    log('get_sources', num_returned=res)
    return res

@app.get("/tags/all", response_model=list|None)
async def get_tags():
    """
    Retrieves the list of tags.
    """
    res = get_all_tags()
    log('get_tags', num_returned=res)
    return res

# initialize_server()


